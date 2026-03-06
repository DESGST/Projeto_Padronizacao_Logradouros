"""
Módulo para seleção, priorização e filtragem de resultados de endereços
"""

import pandas as pd
import re
import logging
from typing import List, Optional, Tuple

# Import das funções de normalização numérica se necessário
from normalizacao_numerica import _normalizar_numeros_por_extenso

# ========== CONFIGURAÇÕES COMPARTILHADAS ==========
TIPOS_VIA = {
    "ACESSO","ALAMEDA","AVENIDA","BECO","CAMINHO","COMPLEXO VIARIO","ESPACO LIVRE","ESPLANADA",
    "ESTRADA","ESCADARIA","ESTRADA PARTICULAR","GALERIA","LADEIRA","LARGO","PASSARELA","PRACA",
    "PRACA PROJETADA","PARQUE","PARQUE ESTADUAL","PARQUE LINEAR","PARQUE MUNICIPAL",
    "PASSAGEM DE PEDESTRE","PASSAGEM PARTICULAR","PASSAGEM SUBTERRÂNEA","PONTILHAO",
    "RUA","RUA PARTICULAR","RUA PROJETADA","RODOVIA","TRAVESSA","TRAVESSA PARTICULAR",
    "VIA DE CIRCULACAO DE PEDESTRES","VIADUTO","VIELA","VIA ELEVADA","VIA ELEVADA DE PEDESTRES",
    "VEREDA","VIELA SANITARIA","VILA","VIELA PROJETADA","VIELA PARTICULAR",
    "AC","AL","AV","BC","CM","CV","EL","EPL","ES","ESC","ESP","GL","LD","LG",
    "PA","PC","PP","PQ","PQE","PQL","PQM","PS","PSP","PSS","PTL",
    "R","RP","RPJ","RV","TV","TVP","VCP","VD","VE","VEL","VEP","VER","VES",
    "VL","VLP","VP","DE","DA","DO"
}

VIAS_PROBLEMATICAS = {
    "EDISON", "RUA EDISON", 
    "SPEERS", "SPEER", "ESTR", "PTE",
    "VILLA", "VILLA-LOBOS"
}

# ========== FUNÇÕES DE PRIORIZAÇÃO ==========

def _priorizar_por_correspondencia_tipo(df: pd.DataFrame, termo_original: str) -> pd.DataFrame:
    """
    Prioriza resultados que correspondem ao tipo de via buscado
    """
    if df.empty or len(df) <= 1:
        return df
    
    termo_upper = termo_original.upper().strip()
    
    # Identificar qual tipo de via está sendo buscado
    tipo_buscado = None
    for tipo_via in TIPOS_VIA:
        if tipo_via in termo_upper and len(tipo_via) > 2:
            tipo_buscado = tipo_via
            break
    
    if not tipo_buscado:
        return df
    
    # Verificar se temos resultados do tipo buscado
    resultados_tipo_buscado = df[df["tipo"].str.upper() == tipo_buscado]
    
    if not resultados_tipo_buscado.empty:
        logging.info(f"🎯 Priorizando {len(resultados_tipo_buscado)} resultado(s) do tipo '{tipo_buscado}'")
        
        if "distancia_km" in df.columns:
            resultados_tipo_buscado = resultados_tipo_buscado.sort_values("distancia_km", ascending=True)
        
        return resultados_tipo_buscado
    
    return df

def _priorizar_por_tipo_especifico(df: pd.DataFrame, tipo_buscado: str) -> pd.DataFrame:
    """
    Prioriza resultados que correspondem ao tipo de via originalmente buscado
    Usado no fallback sem tipo de via
    """
    if df.empty:
        return df
    
    tipo_buscado_upper = tipo_buscado.upper()
    
    # Mapeamento de equivalência de tipos
    equivalencias_tipos = {
        "R": "RUA", "RUA": "R",
        "AV": "AVENIDA", "AVENIDA": "AV", 
        "AL": "ALAMEDA", "ALAMEDA": "AL",
        "TV": "TRAVESSA", "TRAVESSA": "TV",
        "PR": "PRACA", "PRACA": "PR",
        "ES": "ESPLANADA", "ESPLANADA": "ES"
    }
    
    # VERIFICAR se temos resultados do tipo originalmente buscado OU equivalente
    resultados_tipo_original = df[
        (df["tipo"].str.upper() == tipo_buscado_upper) |
        (df["tipo"].str.upper() == equivalencias_tipos.get(tipo_buscado_upper, ""))
    ]
    
    if not resultados_tipo_original.empty:
        logging.info(f"🎯 Fallback: encontrado {len(resultados_tipo_original)} resultado(s) do tipo '{tipo_buscado_upper}'")
        
        # Manter ordem por distância se disponível
        if "distancia_km" in df.columns:
            resultados_tipo_original = resultados_tipo_original.sort_values("distancia_km", ascending=True)
        
        return resultados_tipo_original
    
    # Se não encontrou do tipo original, retornar todos os resultados
    logging.info(f"🔄 Fallback: nenhum resultado do tipo '{tipo_buscado_upper}', mantendo todos os {len(df)} resultados")
    return df

# ========== FUNÇÕES DE FILTRAGEM ==========

def _filtrar_resultados_relevantes(df: pd.DataFrame, termo_original: str) -> pd.DataFrame:
    """
    Filtra resultados mantendo apenas os relevantes
    """
    if df.empty:
        return df
    
    resultados_relevantes = []
    termo_upper = termo_original.upper().strip()
    
    for _, row in df.iterrows():
        # Usar logradouro_pmsp() para criar o nome se não existir
        if 'logradouro_PMSP' in df.columns:
            logradouro = str(row.get('logradouro_PMSP', '')).upper()
        else:
            # Se a coluna não existe, criar o nome do logradouro
            from buscaEndereco_CET_v12_0 import logradouro_pmsp  # Import circular - ajustar conforme necessidade
            logradouro = logradouro_pmsp(row).upper()
        
        # CRITÉRIO FORTE: Termo original aparece COMPLETO no logradouro
        if termo_upper in logradouro:
            resultados_relevantes.append(row)
            continue
            
        # CRITÉRIO: Tokens em comum (para nomes compostos)
        tokens_original = set(re.findall(r'\w+', termo_upper))
        tokens_logradouro = set(re.findall(r'\w+', logradouro))
        tokens_comuns = tokens_original.intersection(tokens_logradouro)
        
        # Se encontrou tokens em comum significativos
        if tokens_comuns and len(tokens_comuns) >= max(1, len(tokens_original) * 0.5):
            resultados_relevantes.append(row)
    
    return pd.DataFrame(resultados_relevantes)

def _filtrar_melhor_embedding(df_embed: pd.DataFrame, endereco_original: str) -> pd.DataFrame:
    """
    Filtra resultados do embedding - CORREÇÃO MAIS FLEXÍVEL
    """
    if df_embed.empty:
        return df_embed
    
    termo_original = endereco_original.upper().strip()
    resultados_relevantes = []
    
    # Termos que indicam estruturas específicas (como "ponte")
    termos_estrutura = {"PONTE", "VIADUTO", "TUNEL", "PASSARELA", "TRAVESSIA"}
    tem_termo_estrutura = any(termo in termo_original for termo in termos_estrutura)
    
    for _, row in df_embed.iterrows():
        logradouro = str(row.get('logradouro_PMSP', '')).upper()
        similaridade = row.get('similaridade', 0)
        
        # Critérios MAIS FLEXÍVEIS para embedding
        tokens_original = set(re.findall(r'\w+', termo_original))
        tokens_logradouro = set(re.findall(r'\w+', logradouro))
        tokens_comuns = tokens_original.intersection(tokens_logradouro)
        
        # Para termos de estrutura (ponte, viaduto), ser mais flexível
        if tem_termo_estrutura:
            criterio_aprovado = (
                similaridade > 50 or  # Limite muito mais baixo
                len(tokens_comuns) >= 1 or  # Apenas 1 token em comum
                any(token in logradouro for token in tokens_original if len(token) > 3)
            )
        else:
            criterio_aprovado = (
                similaridade > 60 or
                len(tokens_comuns) >= 2 or
                termo_original in logradouro
            )
        
        if criterio_aprovado:
            resultados_relevantes.append(row)
            logging.debug(f"✅ Embedding aprovado: {logradouro} (sim: {similaridade})")
    
    if resultados_relevantes:
        df_resultado = pd.DataFrame(resultados_relevantes)
        logging.info(f"🎯 Filtro embedding: {len(df_resultado)} resultados relevantes")
        return df_resultado
    else:
        logging.debug("❌ Nenhum resultado do embedding passou no filtro")
        return pd.DataFrame()

# ========== FUNÇÕES DE AVALIAÇÃO ==========

def _resultados_sao_relevantes(df: pd.DataFrame, endereco_original: str) -> bool:
    """
    Verifica se os resultados são relevantes para o termo buscado
    """
    if df.empty:
        return False
        
    termo_upper = endereco_original.upper().strip()
    
    # Caso especial para rodovias
    if _eh_busca_rodovia(termo_upper):
        return True
    
    # Para termos genéricos, NÃO considerar relevante com poucos resultados
    termos_genericos = {"SPEERS", "SPEER", "ESTR", "PTE", "EDISON"}
    if termo_upper in termos_genericos:
        # Para termos genéricos, só é relevante se tiver múltiplos resultados bons
        if len(df) <= 1:
            return False
        # Verificar se temos diversidade de resultados (não só variações do mesmo logradouro)
        nomes_unicos = set(df['logradouro_PMSP'].str.upper())
        if len(nomes_unicos) <= 1:
            return False
        return True
    
    for _, row in df.iterrows():
        if 'logradouro_PMSP' in df.columns:
            logradouro = str(row.get('logradouro_PMSP', '')).upper()
        else:
            from buscaEndereco_CET_v12_0 import logradouro_pmsp
            logradouro = logradouro_pmsp(row).upper()
        
        # Critério básico: termo aparece no logradouro
        if termo_upper in logradouro or logradouro in termo_upper:
            return True
            
        # Verificar tokens em comum
        tokens_original = set(re.findall(r'\w+', termo_upper))
        tokens_logradouro = set(re.findall(r'\w+', logradouro))
        if tokens_original.intersection(tokens_logradouro):
            return True
    
    return False

def _resultado_eh_suficiente(df_resultado: pd.DataFrame, endereco_original: str) -> bool:
    """
    Verifica se o resultado da busca é SUFICIENTE
    """
    if df_resultado.empty or df_resultado.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO":
        return False
    
    termo_upper = endereco_original.upper().strip()
    
    # Para rodovias, considerar suficiente se encontrou resultados
    if _eh_busca_rodovia(termo_upper) and len(df_resultado) > 0:
        return True
    
    # Para termos genéricos, precisamos de MÚLTIPLOS resultados
    termos_genericos = {"SPEERS", "SPEER", "ESTR", "PTE", "EDISON"}
    if termo_upper in termos_genericos:
        if len(df_resultado) <= 1:
            logging.info(f"🎯 Termo genérico '{termo_upper}' com apenas 1 resultado - NÃO é suficiente")
            return False
        else:
            logging.info(f"🎯 Termo genérico '{termo_upper}' com {len(df_resultado)} resultados - considerado SUFICIENTE")
            return True
    
    # Se encontrou resultados relevantes, é suficiente
    if _resultados_sao_relevantes(df_resultado, endereco_original):
        return True
    
    # Se tem alta similaridade, é suficiente
    if "similaridade" in df_resultado.columns and any(df_resultado["similaridade"] >= 95):
        return True
    
    # Se tem distância muito boa, é suficiente  
    if "distancia_km" in df_resultado.columns and any(df_resultado["distancia_km"] <= 0.1):
        return True
    
    return False

def _precisa_embedding_fallback(df_resultado_principal: pd.DataFrame, endereco_original: str) -> bool:
    """
    Decisão INTELIGENTE sobre quando usar embedding
    """
    # Se não tem resultado principal, precisa de embedding
    if df_resultado_principal.empty or df_resultado_principal.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO":
        logging.info("🔄 Sem resultados principais - usando embedding")
        return True
    
    termo_upper = endereco_original.upper()
    
    # Termos genéricos que SEMPRE devem tentar embedding
    termos_embedding_obrigatorio = {
        "SPEERS", "SPEER", "ESTR", "PTE", "VILLA", "VILLA-LOBOS","EDISON"
    }
    
    # Se é um termo genérico conhecido, SEMPRE usar embedding
    if any(termo == termo_upper for termo in termos_embedding_obrigatorio):
        logging.info(f"🎯 Termo genérico específico detectado: '{termo_upper}' - usando embedding")
        return True
    
    # Para nomes compostos com poucos resultados, usar embedding
    palavras = termo_upper.split()
    if len(palavras) >= 2 and len(df_resultado_principal) <= 1:
        logging.info(f"🎯 Nome composto com poucos resultados: '{termo_upper}' - usando embedding")
        return True
    
    # Se resultados são irrelevantes, precisa de embedding
    if not _resultados_sao_relevantes(df_resultado_principal, endereco_original):
        logging.info(f"🔄 Resultados irrelevantes para '{termo_upper}' - usando embedding")
        return True
    
    logging.debug("✅ Resultados principais suficientes - embedding desnecessário")
    return False

# ========== FUNÇÕES AUXILIARES ==========

def _eh_busca_rodovia(endereco: str) -> bool:
    """
    Verifica se é uma busca por rodovia (SP 070, BR 381, etc.)
    """
    if not endereco:
        return False
    
    padroes_rodovia = [
        r'\b(SP|BR)\s*\d{2,3}\b',
        r'\bSP\s*0?-?\s*15\b',  # Marginal
        r'\bSP\s*0?-?\s*070\b', # Ayrton Senna
        r'\bMARGINAL\b',
        r'\bRODOVIA\b',
        r'\bAYRTON\s*SENNA\b',
        r'\bRADIAL\s*LESTE\b'
    ]
    
    for padrao in padroes_rodovia:
        if re.search(padrao, endereco, re.IGNORECASE):
            return True
    
    return False

def _dataframe_nao_encontrado() -> pd.DataFrame:
    """Retorna DataFrame padrão para 'não encontrado'"""
    return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])

def _combinar_resultados(lista_dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """Combina múltiplos DataFrames removendo duplicatas"""
    if not lista_dataframes:
        return _dataframe_nao_encontrado()
    
    df_combinado = pd.concat(lista_dataframes, ignore_index=True)
    return df_combinado.drop_duplicates(subset=["codlog"], keep="first")

def _eh_resultado_valido(df: pd.DataFrame) -> bool:
    """Verifica se o DataFrame contém resultados válidos"""
    return not df.empty and df.iloc[0]["logradouro_PMSP"] != "NAO ENCONTRADO"

# ========== FUNÇÕES DE PROCESSAMENTO FINAL ==========

def _limpar_colunas_resultado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas internas indesejadas do resultado final
    """
    if df.empty:
        return df
    
    # Colunas que devem ser mantidas
    colunas_desejadas = [
        'codlog', 'logradouro_PMSP', 'latitude_geocode', 'longitude_geocode',
        'distancia_km', 'similaridade', '_fonte_busca',
        'GET', 'DET', 'SUB', 'Distrito_Nome', 'Regiao_Nome', 'Classificacao'
    ]
    
    # Colunas internas que devem ser removidas
    colunas_para_remover = ['tipo', 'titulo', 'preposicao', 'nome', 'LOCAL', 'SIMILARIDADE']
    
    # Manter apenas colunas desejadas que existem no DataFrame
    colunas_finais = [col for col in colunas_desejadas if col in df.columns]
    
    # Remover colunas indesejadas
    for col in colunas_para_remover:
        if col in df.columns and col not in colunas_finais:
            df = df.drop(columns=[col])
    
    return df[colunas_finais] if colunas_finais else df