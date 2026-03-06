from __future__ import annotations
import logging, re, xml.etree.ElementTree as ET
from math import radians, sin, cos, sqrt, atan2
from typing import Any, Dict, Optional, Tuple, List
import time
import requests
import pandas as pd

# Import dos módulos modularizados
from busca_terminais_e_estacoes import filtrar_dataframe_transporte, eh_ponto_transporte
from normalizacao_numerica import _normalizar_numeros_por_extenso
from selecao_resultados import (
    _priorizar_por_correspondencia_tipo,
    _priorizar_por_tipo_especifico, 
    _filtrar_resultados_relevantes,
    _filtrar_melhor_embedding,
    _resultados_sao_relevantes,
    _resultado_eh_suficiente,
    _precisa_embedding_fallback,
    _eh_busca_rodovia,
    _limpar_colunas_resultado
)
from configuracoes import (
    CADASTRO_PATH,
    GEOCODE_ENDERECOS_URL,
    GEOCODE_LATLON_URL, 
    GEOSERVER_WMS_URL,
    _TERMOS_BASE,
    PREPOSICOES_INICIAIS,
    CONTEXTOS_VALIDOS_NORMALIZACAO,
    TIPOS_VIA,
    TIPOS_RODOVIA,
    EQUIVALENCIAS_TIPOS,
    MAPEAMENTO_RODOVIAS,
    MARGINAIS,
    RODOVIAS_ESPECIAIS,
    NOMES_RODOVIAS,
    RODOVIAS_COM_KM,
    TERMOS_ESTRUTURA,
    MAX_TENTATIVAS_REQUISICAO,
    TIMEOUT_REQUISICAO,
    PADROES_RODOVIA,
    RODOVIAS_POR_NOME,
    DETECTOR_RODOVIAS_ESPECIAIS,
    TIPOS_VIA_URBANA,
    ORDEM_PRIORIDADE_FONTES,
    CONFIG_EMBEDDING
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========== FUNÇÕES BASE ==========

def buscar_enderecos(endereco: str) -> pd.DataFrame:
    """Busca endereços no GEOCODE com RETRY"""
    endereco_upper = endereco.upper().strip()
    
    for codigo, nome in MAPEAMENTO_RODOVIAS.items():
        padroes = [rf"\b{codigo}\b", rf"\b{codigo.replace(' ', '')}\b", rf"\b{codigo.replace(' ', '-')}\b"]
        for padrao in padroes:
            if re.search(padrao, endereco_upper, re.IGNORECASE):
                endereco = re.sub(padrao, nome, endereco_upper, flags=re.IGNORECASE)
                logging.info(f"✅ Rodovia substituída: {codigo} → {nome}")
                break
    
    if re.search(r'\bSP\s*0?-?\s*15\b', endereco_upper):
        if "TIETE" in endereco_upper: endereco = "MARGINAL TIETE"
        elif "PINHEIROS" in endereco_upper: endereco = "MARGINAL PINHEIROS"
        else: endereco = "MARGINAL"
        logging.info(f"✅ SP-015 convertido para: {endereco}")
    
    for tentativa in range(MAX_TENTATIVAS_REQUISICAO):
        try:
            response = requests.get(
                GEOCODE_ENDERECOS_URL, 
                params={"endereco": endereco}, 
                timeout=TIMEOUT_REQUISICAO
            )
            response.raise_for_status()
            
            ns = {"ns": "http://tempuri.org/"}
            root = ET.fromstring(response.content)
            enderecos = root.findall("ns:Endereco", ns)
            if not enderecos:
                return pd.DataFrame(columns=["codlog","tipo","titulo","preposicao","nome"])

            registros = []
            for endereco_xml in enderecos:
                log = endereco_xml.find("ns:logradouro1", ns)
                if log is None: 
                    continue
                registros.append({
                    "codlog": log.findtext("ns:codlog", default="", namespaces=ns),
                    "tipo": log.findtext("ns:tipo", default="", namespaces=ns),
                    "titulo": log.findtext("ns:titulo", default="", namespaces=ns),
                    "preposicao": log.findtext("ns:preposicao", default="", namespaces=ns),
                    "nome": log.findtext("ns:nome", default="", namespaces=ns)
                })
            return pd.DataFrame(registros)
            
        except (requests.RequestException, ET.ParseError) as e:
            if tentativa == MAX_TENTATIVAS_REQUISICAO - 1:
                logging.warning(f"❌ GEOCODE falhou após {MAX_TENTATIVAS_REQUISICAO} tentativas para '{endereco}': {e}")
                return pd.DataFrame()
            else:
                logging.debug(f"⚠️ Tentativa {tentativa + 1}/{MAX_TENTATIVAS_REQUISICAO} falhou: {e}")
                time.sleep(1 * (tentativa + 1))
        except Exception as e:
            logging.warning(f"❌ Erro inesperado: {e}")
            return pd.DataFrame()
    
    return pd.DataFrame()

def buscar_latlon_exato(endereco: str) -> tuple[float, float] | None:
    """Busca coordenadas exatas com retry"""
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "http://tempuri.org/buscaLatLonExato"}
    
    partes = endereco.split(',')
    if len(partes) >= 2:
        logradouro = partes[0].strip()
        numero_original = partes[1].strip()
        numero_corrigido = _interpretar_numero_rodovia(numero_original, logradouro)
        
        if numero_corrigido != numero_original:
            endereco = f"{logradouro}, {numero_corrigido}"
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body><buscaLatLonExato xmlns="http://tempuri.org/"><endereco>{endereco}</endereco></buscaLatLonExato></soap:Body>
    </soap:Envelope>"""
    
    for tentativa in range(MAX_TENTATIVAS_REQUISICAO):
        try:
            response = requests.post(GEOCODE_LATLON_URL, data=soap_body, headers=headers, timeout=TIMEOUT_REQUISICAO)
            response.raise_for_status()
            ns = {"soap":"http://schemas.xmlsoap.org/soap/envelope/","ns":"http://tempuri.org/"}
            root = ET.fromstring(response.content)
            ponto = root.find(".//ns:buscaLatLonExatoResult", ns)
            if ponto is None: 
                continue
            x = ponto.findtext("ns:x", default=None, namespaces=ns)
            y = ponto.findtext("ns:y", default=None, namespaces=ns)
            if x and y: 
                return float(y), float(x)
                
        except requests.RequestException as e:
            if tentativa == MAX_TENTATIVAS_REQUISICAO - 1:
                logging.debug(f"❌ LatLon falhou após {MAX_TENTATIVAS_REQUISICAO} tentativas para '{endereco}': {e}")
                return None
            else:
                logging.debug(f"🔄 Tentativa {tentativa + 1}/{MAX_TENTATIVAS_REQUISICAO} falhou para LatLon")
                time.sleep(1 * (tentativa + 1))
        except ET.ParseError as e:
            logging.warning(f"❌ Erro de parse XML para '{endereco}': {e}")
            return None
        except Exception as e:
            logging.warning(f"❌ Erro inesperado em LatLon: {e}")
            return None
    
    return None

def calcular_distancia_km(lat1, lon1, lat2, lon2):
    """Calcula distância em km"""
    if None in [lat1, lon1, lat2, lon2]: return None
    R = 6371.0
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return round(R * 2 * atan2(sqrt(a), sqrt(1 - a)), 3)

def logradouro_pmsp(row) -> str:
    """Monta nome do logradouro"""
    partes = [(row.get("tipo") or "").strip(), (row.get("titulo") or "").strip(), 
              (row.get("preposicao") or "").strip(), (row.get("nome") or "").strip()]
    return " ".join(p for p in partes if p).strip()

# ========== FUNÇÕES DE TRATAMENTO ==========

def _tratar_preposicoes_iniciais(endereco: str) -> list[str]:
    """Trata casos onde endereço começa com preposições"""
    if not endereco:
        return [endereco]
    
    endereco_upper = endereco.upper().strip()
    variacoes = [endereco_upper]
    
    for prep in PREPOSICOES_INICIAIS:
        if endereco_upper.startswith(prep):
            sem_preposicao = endereco_upper[len(prep):].strip()
            if sem_preposicao:
                variacoes.append(sem_preposicao)
            break
    
    seen = set()
    variacoes_unicas = []
    for var in variacoes:
        if var not in seen:
            seen.add(var)
            variacoes_unicas.append(var)
    
    return variacoes_unicas

def _interpretar_numero_rodovia(numero: str, logradouro: str) -> str:
    """Interpretação INTELIGENTE - APENAS para vias de rodovia"""
    if not numero or numero == "0" or not logradouro:
        return numero
    
    logradouro_upper = logradouro.upper()
    partes = logradouro_upper.split()
    
    if not partes or partes[0] not in TIPOS_RODOVIA:
        return numero
    
    for via_principal, faixa_km in RODOVIAS_COM_KM.items():
        if via_principal in logradouro_upper:
            try:
                num = float(numero)
                
                if isinstance(faixa_km, list):
                    for trecho in faixa_km:
                        km_min, km_max = trecho
                        if km_min <= num <= km_max:
                            numero_corrigido = str(int(num * 1000))
                            logging.info(f"🛣️  {via_principal}: KM {num} → Metro {numero_corrigido}")
                            return numero_corrigido
                    logging.debug(f"🔍 Número {num} fora dos trechos válidos do {via_principal}")
                    return numero
                
                else:
                    km_min, km_max = faixa_km
                    if km_min <= num <= km_max:
                        numero_corrigido = str(int(num * 1000))
                        logging.info(f"🛣️  {via_principal}: KM {num} → Metro {numero_corrigido}")
                        return numero_corrigido
                    else:
                        logging.debug(f"🔍 Número {num} fora da faixa {km_min}-{km_max}km da {via_principal}")
                        return numero
                        
            except (ValueError, TypeError):
                return numero
    
    return numero

def remover_tipo_via(endereco: str) -> str:
    """Remove tipo de via E preposição para busca mais flexível"""
    if not endereco:
        return endereco
        
    partes = endereco.strip().upper().split()
    if not partes:
        return endereco.strip()
    
    if partes[0] in TIPOS_VIA:
        if len(partes) > 1 and partes[1] in {"DO", "DA", "DE", "DOS", "DAS"}:
            return " ".join(partes[2:]).strip()
        else:
            return " ".join(partes[1:]).strip()
    
    return endereco.strip()

def detectar_marginal(endereco: str) -> Optional[str]:
    """Detecta se é uma marginal"""
    if not endereco: return None
    e = endereco.upper().strip()
    e = re.sub(r"SP\s*0?-?\s*15", "SP 15", e)
    e = re.sub(r"[-/\s]+", " ", e)
    
    if re.search(r"\bSP 15\b", e):
        if "TIETE" in e: return "TIETE"
        if "PINHEIROS" in e: return "PINHEIROS"
        return "AMBAS"
    if "MARGINAL" in e:
        if "TIETE" in e: return "TIETE"
        if "PINHEIROS" in e: return "PINHEIROS"
        return "AMBAS"
    if "RADIAL LESTE" in e: return "RADIAL LESTE"
    
    return None

# ========== ESTRATÉGIAS DE BUSCA ==========

def _buscar_endereco_exato(endereco: str, numero: str = "") -> pd.DataFrame:
    """Busca EXATA no GEOCODE"""
    endereco_upper = endereco.upper().strip()
    
    if len(endereco_upper.split()) <= 2 and endereco_upper in _TERMOS_BASE:
        df = buscar_enderecos(endereco)
        if not df.empty:
            df_filtrado = _filtrar_resultados_relevantes(df, endereco_upper)
            if not df_filtrado.empty:
                return df_filtrado
    
    if numero and numero != "0":
        df = buscar_enderecos(f"{endereco}, {numero}")
        if not df.empty:
            df_filtrado = _filtrar_resultados_relevantes(df, endereco_upper)
            if not df_filtrado.empty:
                return df_filtrado
    
    return buscar_enderecos(endereco)

def _buscar_endereco_relativo(endereco: str, numero: str = "") -> pd.DataFrame:
    """Busca RELATIVA no GEOCODE (com %)"""
    if numero and numero != "0":
        df = buscar_enderecos(f"{endereco}, {numero}%")
        if not df.empty: return df
    return buscar_enderecos(f"{endereco}%")

def _buscar_em_marginal(tipo_marginal: str, numero: str = "") -> pd.DataFrame:
    """Busca em vias marginais"""
    chave = tipo_marginal.upper()
    if chave == "AMBAS": 
        vias = MARGINAIS["PINHEIROS"] + MARGINAIS["TIETE"] + MARGINAIS["OUTRAS_MARGINAIS"]
    else: 
        vias = MARGINAIS.get(chave, [])
    
    if not vias: 
        return pd.DataFrame()

    resultados = []
    for via in vias:
        df_cand = buscar_enderecos(via)
        if df_cand.empty: 
            continue
        df_cand["logradouro_PMSP"] = df_cand.apply(logradouro_pmsp, axis=1)
        
        latlon_results = []
        for _, row in df_cand.iterrows():
            endereco_com_numero = f"{row['codlog']}, {numero}" if numero else str(row['codlog'])
            latlon = buscar_latlon_exato(endereco_com_numero) or (None, None)
            latlon_results.append(latlon)
        
        df_cand[["latitude", "longitude"]] = pd.DataFrame(latlon_results, index=df_cand.index)
        resultados.append(df_cand)

    if not resultados: 
        return pd.DataFrame()
        
    df_all = pd.concat(resultados, ignore_index=True).drop_duplicates(subset=["codlog"], keep="first")
    df_all["_fonte_busca"] = "VIAS_COMPLEXAS"
    
    return df_all

def _buscar_rodovia_especial(nome_rodovia: str, numero: str = "") -> pd.DataFrame:
    """Busca para rodovias especiais"""
    if nome_rodovia not in RODOVIAS_ESPECIAIS:
        return pd.DataFrame()
    
    vias = RODOVIAS_ESPECIAIS[nome_rodovia]
    if not vias:
        return pd.DataFrame()

    logging.info(f"🛣️  Buscando rodovia especial '{nome_rodovia}' em {len(vias)} vias")
    
    resultados = []
    for via in vias:
        df_cand = buscar_enderecos(via)
        if df_cand.empty:
            continue
            
        df_cand_filtrado = df_cand[df_cand["tipo"].str.upper().isin(TIPOS_RODOVIA)]
        if df_cand_filtrado.empty:
            continue
            
        df_cand_filtrado["logradouro_PMSP"] = df_cand_filtrado.apply(logradouro_pmsp, axis=1)
        
        latlon_results = []
        for _, row in df_cand_filtrado.iterrows():
            logradouro_nome = row['logradouro_PMSP']
            numero_corrigido = _interpretar_numero_rodovia(numero, logradouro_nome)
            
            endereco_com_numero = f"{row['codlog']}, {numero_corrigido}" if numero else str(row['codlog'])
            latlon = buscar_latlon_exato(endereco_com_numero) or (None, None)
            latlon_results.append(latlon)
        
        df_cand_filtrado[["latitude", "longitude"]] = pd.DataFrame(latlon_results, index=df_cand_filtrado.index)
        resultados.append(df_cand_filtrado)

    if not resultados:
        logging.info(f"❌ Rodovia especial '{nome_rodovia}' não retornou resultados válidos")
        return pd.DataFrame()
        
    df_all = pd.concat(resultados, ignore_index=True).drop_duplicates(subset=["codlog"], keep="first")
    df_all["_fonte_busca"] = "VIAS_COMPLEXAS"
    
    logging.info(f"✅ Rodovia especial '{nome_rodovia}': {len(df_all)} resultados RODOVIÁRIOS encontrados")
    return df_all

def _eh_nome_rodovia_sem_tipo(endereco: str) -> bool:
    """Verifica se é nome de rodovia mesmo sem o tipo de via"""
    if not endereco:
        return False
    
    endereco_upper = endereco.upper().strip()
    
    if endereco_upper in NOMES_RODOVIAS:
        return True
    
    for padrao in PADROES_RODOVIA:
        if re.search(padrao, endereco_upper, re.IGNORECASE):
            return True
    
    return False

# ========== FUNÇÕES AUXILIARES DO FLUXO PRINCIPAL ==========

def _buscar_por_embedding(endereco: str, numero: str = "") -> pd.DataFrame:
    """Fallback usando embedding"""
    try:
        from busca_enderecos_simplificado_v5 import busca_candidatos_df
        
        logging.info(f"🔍 Executando embedding para: '{endereco}'")
        
        if len(endereco.strip().split()) <= 3:
            config = CONFIG_EMBEDDING["TERMOS_CURTOS"]
            top_k = config["top_k"]
            min_sim = config["min_sim"]
        else:
            config = CONFIG_EMBEDDING["TERMOS_LONGOS"]
            top_k = config["top_k"]
            min_sim = config["min_sim"]
        
        candidatos = busca_candidatos_df(
            endereco=endereco,
            cadastro_path=CADASTRO_PATH,
            top_k=top_k,
            min_sim=min_sim
        )
        
        if candidatos is None or candidatos.empty:
            logging.debug("❌ Embedding não retornou resultados")
            return pd.DataFrame()
        
        logging.info(f"📊 Embedding retornou {len(candidatos)} candidatos iniciais")
        
        coletados = []
        for _, row in candidatos.iterrows():
            try:
                codlog = str(row["codlog"]).strip()
                similaridade = int(row["SIMILARIDADE"])
                nome_embedding = str(row["LOCAL"])
                
                df_log = buscar_enderecos(codlog)
                if df_log.empty:
                    df_log = buscar_enderecos(nome_embedding)
                
                if not df_log.empty:
                    log_info = df_log.iloc[0]
                    logradouro_oficial = logradouro_pmsp(log_info)
                    codlog_correto = str(log_info["codlog"])
                else:
                    logradouro_oficial = nome_embedding
                    codlog_correto = codlog
                
                endereco_completo = f"{codlog_correto}, {numero}" if numero else codlog_correto
                latlon = buscar_latlon_exato(endereco_completo)
                
                if latlon and latlon[0] is not None and latlon[1] is not None:
                    df_candidato = pd.DataFrame([{
                        "codlog": codlog_correto,
                        "logradouro_PMSP": logradouro_oficial,
                        "latitude_geocode": latlon[0],
                        "longitude_geocode": latlon[1],
                        "similaridade": similaridade,
                        "_fonte_busca": "EMBEDDING",
                        "tipo": log_info.get("tipo", "") if 'log_info' in locals() else "",
                        "titulo": log_info.get("titulo", "") if 'log_info' in locals() else "",
                        "preposicao": log_info.get("preposicao", "") if 'log_info' in locals() else "",
                        "nome": log_info.get("nome", "") if 'log_info' in locals() else ""
                    }])
                    coletados.append(df_candidato)
                    logging.debug(f"✅ Embedding: {logradouro_oficial} (similaridade: {similaridade})")
                    
            except Exception as e:
                logging.debug(f"⚠️ Erro processando candidato do embedding: {e}")
                continue
        
        if coletados:
            df_final = pd.concat(coletados, ignore_index=True)
            df_final = df_final.drop_duplicates(subset=["codlog"], keep="first")
            logging.info(f"🎯 Embedding: {len(df_final)} resultados válidos processados")
            return df_final
        else:
            logging.debug("❌ Nenhum candidato do embedding pôde ser processado")
            return pd.DataFrame()
            
    except Exception as e:
        logging.warning(f"⚠️ Fallback por embedding falhou: {e}")
        import traceback
        logging.debug(f"Detalhes do erro: {traceback.format_exc()}")
        return pd.DataFrame()

def _adicionar_metadados_busca(df: pd.DataFrame, fonte: str, similaridade: int) -> pd.DataFrame:
    """Adiciona metadados de busca ao DataFrame"""
    df_result = df.copy()
    df_result["_fonte_busca"] = fonte
    df_result["similaridade"] = similaridade
    return df_result

def _processar_coordenadas_resultado(df: pd.DataFrame, endereco: str, numero: str, lat_origem: float, lon_origem: float, max_candidatos: int) -> pd.DataFrame:
    """Processa coordenadas e ordena resultados"""
    if df.empty:
        return df
    
    if 'logradouro_PMSP' not in df.columns:
        df['logradouro_PMSP'] = df.apply(logradouro_pmsp, axis=1)
    
    if not df.empty and 'logradouro_PMSP' in df.columns:
        if df.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO":
            return df
    
    colunas_coordenadas = {
        'latitude': 'latitude_geocode',
        'longitude': 'longitude_geocode', 
        'lat': 'latitude_geocode',
        'lon': 'longitude_geocode',
        'lng': 'longitude_geocode'
    }
    
    for col_antiga, col_nova in colunas_coordenadas.items():
        if col_antiga in df.columns and col_nova not in df.columns:
            df = df.rename(columns={col_antiga: col_nova})
    
    for i, row in df.iterrows():
        if pd.isna(row.get("latitude_geocode")) or pd.isna(row.get("longitude_geocode")):
            logradouro_nome = row.get('logradouro_PMSP', '') or logradouro_pmsp(row)
            numero_corrigido = _interpretar_numero_rodovia(numero, logradouro_nome)
            
            endereco_busca = f"{row['codlog']}, {numero_corrigido}" if numero else row['codlog']
            latlon = buscar_latlon_exato(endereco_busca) or (None, None)
            df.at[i, "latitude_geocode"] = latlon[0]
            df.at[i, "longitude_geocode"] = latlon[1]
    
    if lat_origem and lon_origem:
        df["distancia_km"] = df.apply(
            lambda r: calcular_distancia_km(lat_origem, lon_origem, r["latitude_geocode"], r["longitude_geocode"]), 
            axis=1
        )
        df = df.sort_values("distancia_km", ascending=True)
        
        df_priorizado = _priorizar_por_correspondencia_tipo(df, endereco)
        
        if not df_priorizado.empty and len(df_priorizado) >= 1:
            df = df_priorizado
    
    else:
        df["distancia_km"] = None
        if "similaridade" in df.columns and "_fonte_busca" in df.columns:
            df["_ordem_fonte"] = df["_fonte_busca"].map(ORDEM_PRIORIDADE_FONTES).fillna(6)
            df = df.sort_values(["similaridade", "_ordem_fonte"], ascending=[False, True]).drop("_ordem_fonte", axis=1)
    
    return df.head(max_candidatos)

# ========== FUNÇÕES DE DETECÇÃO DE VIAS ESPECIAIS ==========

def _detectar_rodovia_especial(endereco: str) -> Optional[str]:
    """Detecta rodovias especiais que precisam de tratamento diferenciado"""
    if not endereco:
        return None
    
    endereco_upper = endereco.upper().strip()
    partes = endereco_upper.split()
    
    if partes and partes[0] in TIPOS_VIA_URBANA:
        return None
    
    for rodovia, termos in DETECTOR_RODOVIAS_ESPECIAIS.items():
        for termo in termos:
            if termo == endereco_upper or f" {termo} " in f" {endereco_upper} ":
                logging.debug(f"🔍 Rodovia especial '{rodovia}' detectada via termo '{termo}'")
                return rodovia
    
    return None

def _buscar_rodovia_direta(endereco: str, numero: str = "") -> pd.DataFrame:
    """Busca direta por rodovias"""
    endereco_upper = endereco.upper().strip()
    
    if not _eh_nome_rodovia_sem_tipo(endereco_upper):
        return pd.DataFrame()
    
    partes = endereco_upper.split()
    if partes and partes[0] in TIPOS_VIA_URBANA:
        return pd.DataFrame()
    
    if endereco_upper in RODOVIAS_POR_NOME:
        codigo = RODOVIAS_POR_NOME[endereco_upper]
        df_rodovia = buscar_enderecos(codigo)
        if not df_rodovia.empty:
            logradouro_real = logradouro_pmsp(df_rodovia.iloc[0])
            numero_corrigido = _interpretar_numero_rodovia(numero, logradouro_real)
            
            endereco_completo = f"{codigo}, {numero_corrigido}" if numero else codigo
            latlon = buscar_latlon_exato(endereco_completo) or (None, None)
            
            df_result = pd.DataFrame([{
                "codlog": codigo,
                "logradouro_PMSP": logradouro_real,
                "latitude_geocode": latlon[0],
                "longitude_geocode": latlon[1],
                "similaridade": 100,
                "_fonte_busca": "RODOVIA"
            }])
            logging.info(f"✅ Rodovia '{endereco}' encontrada: {logradouro_real}")
            return df_result
    
    for codigo, nome in MAPEAMENTO_RODOVIAS.items():
        padroes = [rf"\b{codigo}\b", rf"\b{codigo.replace(' ', '')}\b", rf"\b{codigo.replace(' ', '-')}\b"]
        for padrao in padroes:
            if re.search(padrao, endereco_upper, re.IGNORECASE):
                logging.info(f"🛣️  Buscando rodovia {codigo} pelo código {nome}")
                
                if nome in RODOVIAS_ESPECIAIS:
                    return _buscar_rodovia_especial(nome, numero)
                else:
                    df_rodovia = buscar_enderecos(nome)
                    if not df_rodovia.empty:
                        logradouro_real = logradouro_pmsp(df_rodovia.iloc[0])
                        numero_corrigido = _interpretar_numero_rodovia(numero, logradouro_real)
                        
                        endereco_completo = f"{nome}, {numero_corrigido}" if numero else nome
                        latlon = buscar_latlon_exato(endereco_completo) or (None, None)
                        
                        df_result = pd.DataFrame([{
                            "codlog": nome,
                            "logradouro_PMSP": logradouro_real,
                            "latitude_geocode": latlon[0],
                            "longitude_geocode": latlon[1],
                            "similaridade": 100,
                            "_fonte_busca": "RODOVIA"
                        }])
                        logging.info(f"✅ Rodovia {codigo} encontrada: {logradouro_real}")
                        return df_result
                break
    
    return pd.DataFrame()

# ========== FUNÇÕES PRINCIPAIS DE BUSCA ==========

def _buscar_direta_inteligente(endereco: str, numero: str = "", lat_origem: float = None, lon_origem: float = None) -> pd.DataFrame:
    """Busca DIRETA usando toda a inteligência disponível"""
    logging.debug(f"🔍 _buscar_direta_inteligente: '{endereco}'")
    
    endereco_upper = endereco.upper().strip()
    if endereco_upper in _TERMOS_BASE:
        logging.info(f"🎯 VIA PROBLEMÁTICA: '{endereco}' - buscando APENAS por embedding")
        df_embedding = _buscar_por_embedding(endereco, numero)
        if not df_embedding.empty:
            return _processar_coordenadas_resultado(df_embedding, endereco, numero, lat_origem, lon_origem, 35)
        return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])
    
    rodovia_especial = _detectar_rodovia_especial(endereco)
    if rodovia_especial:
        logging.info(f"🎯 Rodovia especial detectada: {rodovia_especial} - forçando busca especial")
        df_especial = _buscar_rodovia_especial(rodovia_especial, numero)
        if not df_especial.empty:
            return df_especial
    
    if _eh_nome_rodovia_sem_tipo(endereco):
        logging.info(f"🎯 Nome de rodovia detectado: '{endereco}' - buscando APENAS como rodovia")
        df_rodovia = _buscar_rodovia_direta(endereco, numero)
        if not df_rodovia.empty:
            return df_rodovia
        else:
            logging.info(f"❌ Rodovia '{endereco}' não encontrada - não buscando vias urbanas")
            return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])
  
    tipo_marginal = detectar_marginal(endereco)
    if tipo_marginal:
        logging.debug(f"🎯 Marginal detectada: {tipo_marginal}")
        df = _buscar_em_marginal(tipo_marginal, numero)
        if not df.empty:
            df = df.rename(columns={"latitude": "latitude_geocode", "longitude": "longitude_geocode"})
            df = _adicionar_metadados_busca(df, "VIAS_COMPLEXAS", 100)
            return _processar_coordenadas_resultado(df, endereco, numero, lat_origem, lon_origem, 35)
    
    df_rodovia = _buscar_rodovia_direta(endereco, numero)
    if not df_rodovia.empty:
        logging.debug("🎯 Rodovia encontrada")
        return df_rodovia
    
    candidatos = []
    
    logging.debug(f"🔍 Buscando exato: '{endereco}'")
    df_exato = _buscar_endereco_exato(endereco, numero)
    logging.debug(f"📊 Resultados exatos: {len(df_exato)}")
    
    if not df_exato.empty:
        if 'logradouro_PMSP' not in df_exato.columns:
            df_exato['logradouro_PMSP'] = df_exato.apply(logradouro_pmsp, axis=1)
        
        df_exato_filtrado = _filtrar_resultados_relevantes(df_exato, endereco)
        if not df_exato_filtrado.empty:
            df_exato_filtrado = _adicionar_metadados_busca(df_exato_filtrado, "EXATO", 100)
            candidatos.append(df_exato_filtrado)
            logging.info(f"✅ {len(df_exato_filtrado)} resultado(s) EXATO(s) relevantes") 
        else:
            logging.debug("❌ Resultados exatos filtrados como irrelevantes")
    else:
        logging.debug("❌ Nenhum resultado exato encontrado")
    
    if not candidatos or not _resultados_sao_relevantes(pd.concat(candidatos, ignore_index=True), endereco):
        logging.debug(f"🔍 Buscando relativo: '{endereco}%'")
        df_rel = _buscar_endereco_relativo(endereco, numero)
        logging.debug(f"📊 Resultados relativos: {len(df_rel)}")
        
        if not df_rel.empty:
            if 'logradouro_PMSP' not in df_rel.columns:
                df_rel['logradouro_PMSP'] = df_rel.apply(logradouro_pmsp, axis=1)
            
            df_rel_filtrado = _filtrar_resultados_relevantes(df_rel, endereco)
            if not df_rel_filtrado.empty:
                df_rel_filtrado = _adicionar_metadados_busca(df_rel_filtrado.head(8), "RELATIVO", 85)
                candidatos.append(df_rel_filtrado)
                logging.info(f"✅ {len(df_rel_filtrado)} resultado(s) RELATIVO(s)")
            else:
                logging.debug("❌ Resultados relativos filtrados como irrelevantes")
        else:
            logging.debug("❌ Nenhum resultado relativo encontrado")
    
    if candidatos:
        df_result = pd.concat(candidatos, ignore_index=True).drop_duplicates(subset=["codlog"], keep="first")
        logging.debug(f"🎯 Resultados finais da busca direta: {len(df_result)}")
        return _processar_coordenadas_resultado(df_result, endereco, numero, lat_origem, lon_origem, 35)
    
    logging.debug("❌ Nenhum resultado encontrado na busca direta")
    return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])

# ========== FUNÇÃO PRINCIPAL DE BUSCA ==========

def buscar_endereco_candidatos(endereco: str, numero: str = "", lat_origem: float = None, lon_origem: float = None, max_candidatos: int = 35) -> pd.DataFrame:
    """
    Busca RÁPIDA OTIMIZADA - CORREÇÃO: Verificar filtro ANTES da busca
    """
    if not endereco or endereco.strip() == "":
        return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])
    
    if _eh_terminal_estacao_metro(endereco):
        logging.info(f"🚫 Busca por ponto de transporte bloqueada: '{endereco}'")
        return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])
    
    endereco_upper = endereco.upper().strip()
    if endereco_upper in _TERMOS_BASE:
        logging.info(f"🎯 VIA PROBLEMÁTICA DETECTADA: '{endereco}' - usando APENAS embedding")
        df_embedding = _buscar_por_embedding(endereco, numero)
        if not df_embedding.empty:
            df_embedding_processado = _processar_coordenadas_resultado(
                df_embedding, endereco, numero, lat_origem, lon_origem, max_candidatos
            )
            logging.info(f"✅ Embedding para '{endereco}' trouxe {len(df_embedding_processado)} resultados")
            return _limpar_colunas_resultado(df_embedding_processado)
        else:
            logging.info(f"❌ Embedding não encontrou resultados para '{endereco}'")
            return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])
    
    endereco_normalizado = _normalizar_numeros_por_extenso(endereco)
    if endereco_normalizado != endereco.upper():
        logging.info(f"🔤 Endereço normalizado: '{endereco}' → '{endereco_normalizado}'")
        endereco = endereco_normalizado
            
    variacoes_endereco = _tratar_preposicoes_iniciais(endereco)
    if len(variacoes_endereco) > 1:
        logging.info(f"🔄 Variações de busca: {variacoes_endereco}")
    
    logging.info(f"🔍 Buscando: '{endereco}', Número: '{numero}'")
    
    todos_candidatos = []
    
    for variacao in variacoes_endereco:
        df_variacao = _buscar_direta_inteligente(variacao, numero, lat_origem, lon_origem)
        if not df_variacao.empty and df_variacao.iloc[0]["logradouro_PMSP"] != "NAO ENCONTRADO":
            todos_candidatos.append(df_variacao)
    
    if todos_candidatos:
        df_combinado = pd.concat(todos_candidatos, ignore_index=True)
        df_combinado = df_combinado.drop_duplicates(subset=["codlog"], keep="first")
        
        df_combinado = _filtrar_terminal_estacao_metro(df_combinado)
        
        df_principal = _processar_coordenadas_resultado(df_combinado, endereco, numero, lat_origem, lon_origem, max_candidatos)
    else:
        df_principal = pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])        
    
    if _resultado_eh_suficiente(df_principal, endereco):
        logging.info("✅ Resultado principal considerado SUFICIENTE")
        return _limpar_colunas_resultado(df_principal.head(max_candidatos))
      
    logging.info("🔄 Resultado principal insuficiente, aplicando fallbacks...")
    
    candidatos_fallback = []
    
    if not _resultados_sao_relevantes(df_principal, endereco):
        sem_tipo = remover_tipo_via(endereco)
        if sem_tipo != endereco:
            logging.debug(f"🔍 Tentando fallback sem tipo: '{sem_tipo}'")
            df_sem_tipo = _buscar_direta_inteligente(sem_tipo, numero, lat_origem, lon_origem)
            logging.debug(f"📊 Resultados sem tipo: {len(df_sem_tipo)}")
            
            if not df_sem_tipo.empty and _resultados_sao_relevantes(df_sem_tipo, endereco):
                if 'logradouro_PMSP' not in df_sem_tipo.columns:
                    df_sem_tipo['logradouro_PMSP'] = df_sem_tipo.apply(logradouro_pmsp, axis=1)
                
                df_sem_tipo["_fonte_busca"] = "EXATO_SEM_TIPO"
                df_sem_tipo["similaridade"] = 90
                
                tipo_original = endereco.upper().split()[0]
                if tipo_original in TIPOS_VIA:
                    df_sem_tipo_priorizado = _priorizar_por_tipo_especifico(df_sem_tipo, tipo_original)
                    if not df_sem_tipo_priorizado.empty:
                        df_sem_tipo = df_sem_tipo_priorizado
                        logging.info(f"🎯 Fallback: priorizando resultados do tipo '{tipo_original}'")
                
                df_sem_tipo_processado = _processar_coordenadas_resultado(
                    df_sem_tipo, endereco, numero, lat_origem, lon_origem, max_candidatos
                )
                candidatos_fallback.append(df_sem_tipo_processado)
                logging.info(f"✅ Fallback sem tipo de via trouxe {len(df_sem_tipo_processado)} resultados relevantes")
    
    if _precisa_embedding_fallback(df_principal, endereco) and not candidatos_fallback:
        logging.info("🔄 Problema não resolvido, tentando embedding...")
        df_embedding = _buscar_por_embedding(endereco, numero)
        
        if not df_embedding.empty:
            df_embedding_processado = _processar_coordenadas_resultado(
                df_embedding, endereco, numero, lat_origem, lon_origem, max_candidatos
            )
            
            df_embedding_filtrado = _filtrar_melhor_embedding(df_embedding_processado, endereco)
            
            if not df_embedding_filtrado.empty:
                tipo_original = endereco.upper().split()[0]
                if tipo_original in TIPOS_VIA:
                    df_embedding_priorizado = _priorizar_por_tipo_especifico(df_embedding_filtrado, tipo_original)
                    if not df_embedding_priorizado.empty:
                        df_embedding_filtrado = df_embedding_priorizado
                        logging.info(f"🎯 Embedding: priorizando resultados do tipo '{tipo_original}'")
                
                candidatos_fallback.append(df_embedding_filtrado)
                logging.info(f"✅ Embedding trouxe {len(df_embedding_filtrado)} resultados relevantes")
    
    if candidatos_fallback:
        todos_resultados = [df_principal] + candidatos_fallback
        df_combinado = pd.concat(todos_resultados, ignore_index=True)
        df_combinado = df_combinado.drop_duplicates(subset=["codlog"], keep="first")
        
        if len(df_combinado) > 1 and "NAO ENCONTRADO" in df_combinado["logradouro_PMSP"].values:
            df_combinado = df_combinado[df_combinado["logradouro_PMSP"] != "NAO ENCONTRADO"]
        
        resultado_final = _processar_coordenadas_resultado(df_combinado, endereco, numero, lat_origem, lon_origem, max_candidatos)
        logging.info(f"✅ Resultado final combinado: {len(resultado_final)} candidatos")
        return _limpar_colunas_resultado(resultado_final)
    
    logging.info("⚠️  Nenhum fallback trouxe melhorias significativas")
    return _limpar_colunas_resultado(df_principal.head(max_candidatos))

# ========== FUNÇÕES DE FILTRAGEM E DETECÇÃO ==========

def _filtrar_terminal_estacao_metro(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra Terminais, Estações e Metrô - CORREÇÃO: NÃO filtrar rodovias
    """
    if df.empty:
        return df
    
    df_result = df.copy()
    
    for idx, row in df.iterrows():
        logradouro = str(row.get('logradouro_PMSP', ''))
        
        if _eh_rodovia(logradouro):
            continue
        
        if _eh_terminal_estacao_metro(logradouro):
            logging.info(f"🚫 Terminal/Estação detectado: '{logradouro}'")
            df_result.at[idx, "logradouro_PMSP"] = "NAO ENCONTRADO"
            df_result.at[idx, "codlog"] = ""
            df_result.at[idx, "latitude_geocode"] = None
            df_result.at[idx, "longitude_geocode"] = None
    
    return df_result

def _eh_rodovia(logradouro: str) -> bool:
    """
    Verifica se é uma via de rodovia - CORREÇÃO PARA EVITAR FALSO POSITIVO
    """
    if not logradouro:
        return False
    
    logradouro_upper = logradouro.upper().strip()
    
    partes = logradouro_upper.split()
    if partes and partes[0] in TIPOS_RODOVIA:
        logging.debug(f"🎯 Via de rodovia (não filtrar): '{logradouro}'")
        return True
    
    rodovias_principais = {
        "AYRTON SENNA", "ANHANGUERA", "ANCHIETA", "IMIGRANTES",
        "FERNÃO DIAS", "BANDEIRANTES", "RAPOSO TAVARES", 
        "PRESIDENTE DUTRA", "RODOANEL", "MARIO COVAS",
        "MARGINAL TIETE", "MARGINAL PINHEIROS", "RADIAL LESTE",
        "TRABALHADORES"
    }
    
    for rodovia in rodovias_principais:
        if rodovia in logradouro_upper:
            logging.debug(f"🎯 Nome de rodovia (não filtrar): '{logradouro}'")
            return True
    
    return False

def _eh_terminal_estacao_metro(logradouro: str) -> bool:
    """
    Detecta se o logradouro é um Terminal, Estação ou Metrô - CORREÇÃO PRECISA
    """
    if not logradouro:
        return False
    
    logradouro_upper = logradouro.upper().strip()
    
    padroes_terminais = [
        r'^(TERMINAL|TERM|ESTAÇÃO|ESTACAO|EST|METRÔ|METRO)\s+[A-Z]',
        r'\b(TERMINAL|TERM|ESTAÇÃO|ESTACAO|EST|METRÔ|METRO)\s+[A-Z]',
    ]
    
    for padrao in padroes_terminais:
        if re.search(padrao, logradouro_upper, re.IGNORECASE):
            logging.debug(f"🎯 Ponto de transporte detectado: '{logradouro}'")
            return True
    
    palavras_chave = {
        "TERMINAL", "TERM", "ESTAÇÃO", "ESTACAO", "EST", 
        "METRÔ", "METRO", "STATION", "TERMINUS"
    }
    
    palavras_log = set(re.findall(r'\b\w+\b', logradouro_upper))
    if palavras_log.intersection(palavras_chave):
        logging.debug(f"🎯 Palavra-chave de transporte: '{logradouro}'")
        return True
    
    return False

# ========== FUNÇÕES DE ENRIQUECIMENTO GEOSERVER ==========

def enriquecer_candidatos_geoserver(df_candidatos: pd.DataFrame) -> pd.DataFrame:
    """Adiciona dados do GEOSERVER com RETRY para oscilações de rede"""
    if df_candidatos.empty or df_candidatos.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO":
        return df_candidatos
    
    GEOSERVER_LAYERS = [
        "sqlProducao:gets_lg", "sqlProducao:dets_lg", "sqlProducao:mdcSubPrefeitura",
        "sqlProducao:mdcDistrito", "sqlProducao:regiao5_lg", "sqlProducao:ClassVia"
    ]
    
    def obter_dados_ponto(lat: float, lon: float) -> dict:
        layers = ",".join(GEOSERVER_LAYERS)
        params = {
            "SERVICE":"WMS","VERSION":"1.1.1","REQUEST":"GetFeatureInfo",
            "LAYERS":layers,"QUERY_LAYERS":layers,"STYLES":"",
            "SRS":"EPSG:4326", "BBOX": f"{lon-0.0015},{lat-0.0015},{lon+0.0015},{lat+0.0015}",
            "WIDTH":"101","HEIGHT":"101","X":"50","Y":"50",
            "INFO_FORMAT":"application/json","FEATURE_COUNT":"50"
        }
        
        # ✅ AGORA COM RETRY
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                r = requests.get(GEOSERVER_WMS_URL, params=params, timeout=30)
                r.raise_for_status()
                if not r.headers.get("content-type","").startswith("application/json"):
                    continue
                    
                data = r.json()
                out = {}
                
                def _infer_layer_name(feat: dict) -> str:
                    fid = str(feat.get("id",""))
                    return fid.split(".",1)[0] if "." in fid else fid

                for f in data.get("features", []):
                    props = f.get("properties", {}) or {}
                    layer = _infer_layer_name(f)

                    # Fixed: layer names changed from gets_lg to vwGets, dets_lg to vwDets
                    if "gets" in layer.lower() or "vwgets" in layer.lower(): 
                        out["GET"] = props.get("sigla")  # Changed from siglaGet
                    if "dets" in layer.lower() or "vwdets" in layer.lower(): 
                        out["DET"] = props.get("sigla")
                    if layer.endswith("mdcSubPrefeitura") or "subprefeitura" in layer.lower(): 
                        out["SUB"] = props.get("sigla2")
                    if layer.endswith("mdcDistrito") or "distrito" in layer.lower(): 
                        out["Distrito_Nome"] = props.get("Nome_distr")
                    if layer.endswith("regiao5") or "regiao5" in layer.lower(): 
                        out["Regiao_Nome"] = props.get("nome")
                    if layer.endswith("ClassVia") or "classvia" in layer.lower(): 
                        out["Classificacao"] = props.get("Classificacao")
                return out
                
            except (requests.RequestException, ValueError) as e:
                if tentativa == max_tentativas - 1:
                    logging.debug(f"❌ GEOSERVER falhou após {max_tentativas} tentativas")
                    return {}
                else:
                    time.sleep(1 * (tentativa + 1))  # Backoff
            except Exception as e:
                logging.warning(f"❌ Erro inesperado: {e}")
                return {}
        
        return {}
    
    # ✅ O resto da função mantém IGUAL
    df_enriquecido = df_candidatos.copy()
    for i, row in df_enriquecido.iterrows():
        if pd.notna(row["latitude_geocode"]) and pd.notna(row["longitude_geocode"]):
            info = obter_dados_ponto(row["latitude_geocode"], row["longitude_geocode"])
            for col in ['GET', 'DET', 'SUB', 'Distrito_Nome', 'Regiao_Nome', 'Classificacao']:
                df_enriquecido.at[i, col] = info.get(col)
    
    return df_enriquecido

# ========== FUNÇÕES PRINCIPAIS DE INTERFACE ==========

def buscar_endereco_enriquecido(endereco: str, numero: str = "", lat_origem: float = None, lon_origem: float = None, max_candidatos: int = 35) -> pd.DataFrame:
    """
    Busca COMPLETA - com enriquecimento GEOSERVER
    """
    df_candidatos = buscar_endereco_candidatos(endereco, numero, lat_origem, lon_origem, max_candidatos)
    return enriquecer_candidatos_geoserver(df_candidatos)

# ========== FUNÇÕES DE COMPATIBILIDADE ==========

buscar_endereco_com_coordenadas = buscar_endereco_candidatos
buscar_endereco_completo = buscar_endereco_enriquecido