import pandas as pd
import requests
import logging
import time
import os
import concurrent
inicio_processo = time.time()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from configuracoes import (
    GEOSERVER_WMS_URL,
    MAX_TENTATIVAS_REQUISICAO,
    TIMEOUT_REQUISICAO,
)
import datetime # Adicionei para formatar o tempo bonito

# --- ⏱️ INICIA O CRONÔMETRO AQUI ---
print("⏱️ Cronômetro iniciado...")
from buscaEndereco_CET_v12_0 import buscar_endereco_candidatos
import concurrent.futures

def enriquecer_candidatos_geoserver(df_candidatos: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
    """
    Versão OTIMIZADA com ThreadPoolExecutor.
    Faz várias requisições ao mesmo tempo para acelerar o processo.
    """
    
    if df_candidatos.empty:
        return df_candidatos
    
    # Camadas que queremos consultar
    GEOSERVER_LAYERS = [
        "sqlProducao:gets_lg", "sqlProducao:dets_lg", "sqlProducao:mdcSubPrefeitura",
        "sqlProducao:mdcDistrito", "sqlProducao:regiao5_lg", "sqlProducao:ClassVia"
    ]
    
    # Função que processa UMA linha (isolada para rodar em paralelo)
    def processar_linha(args):
        i, lat, lon = args
        
        # Se lat/lon inválidos, retorna vazio
        if pd.isna(lat) or pd.isna(lon):
            return i, {}

        layers = ",".join(GEOSERVER_LAYERS)
        params = {
            "SERVICE":"WMS","VERSION":"1.1.1","REQUEST":"GetFeatureInfo",
            "LAYERS":layers,"QUERY_LAYERS":layers,"STYLES":"",
            "SRS":"EPSG:4326", "BBOX": f"{lon-0.0015},{lat-0.0015},{lon+0.0015},{lat+0.0015}",
            "WIDTH":"101","HEIGHT":"101","X":"50","Y":"50",
            "INFO_FORMAT":"application/json","FEATURE_COUNT":"50"
        }
        
        # Retry logic
        for tentativa in range(MAX_TENTATIVAS_REQUISICAO):
            try:
                r = requests.get(GEOSERVER_WMS_URL, params=params, timeout=TIMEOUT_REQUISICAO)
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

                    if "gets" in layer.lower() or "vwgets" in layer.lower(): 
                        out["GET"] = props.get("sigla")
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
                return i, out
                
            except Exception:
                time.sleep(1) # Espera um pouco antes de tentar de novo
        
        return i, {} # Falhou após todas tentativas
    
    # Preparação para o paralelismo
    df_enriquecido = df_candidatos.copy()
    total = len(df_enriquecido)
    print(f"🚀 Iniciando processamento PARALELO de {total} linhas com {max_workers} threads...")
    
    # Monta a lista de tarefas (tuplas com índice, lat, lon)
    tarefas = []
    for i, row in df_enriquecido.iterrows():
        # Só adiciona na fila se tiver coordenada
        if pd.notna(row.get("latitude_geocode")):
            tarefas.append((i, row["latitude_geocode"], row["longitude_geocode"]))

    # Executa em paralelo
    # max_workers=8 é um número seguro para não derrubar o servidor, mas rápido o suficiente
    contador = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # O executor.map mantém a ordem? Não necessariamente o retorno, mas aqui usamos future.result
        # Vamos usar as_completed para ver o progresso
        futuros = {executor.submit(processar_linha, t): t[0] for t in tarefas}
        
        for future in concurrent.futures.as_completed(futuros):
            idx, resultado = future.result()
            
            # Atualiza o DataFrame com o resultado
            if resultado:
                for col, val in resultado.items():
                    df_enriquecido.at[idx, col] = val
            
            contador += 1
            if contador % 100 == 0:
                print(f"   Progresso: {contador}/{len(tarefas)}...", end='\r')
                
    print(f"\n✅ Processamento paralelo finalizado!")
    return df_enriquecido
def atualizar_tudo_geoserver(df_principal: pd.DataFrame) -> pd.DataFrame:
    """
    MODO FORÇADO:
    1. Ignora se os campos já estão preenchidos.
    2. Pega TODAS as linhas que possuem Latitude e Longitude válidas.
    3. Consulta o GeoServer e SOBRESCREVE os dados de enriquecimento (incluindo Classificação).
    """
    
    # 1. Colunas que serão atualizadas/criadas
    colunas_alvo = ['GET', 'DET', 'SUB', 'Distrito_Nome', 'Regiao_Nome', 'Classificacao']
    
    # 2. Garante que as colunas existem na tabela
    for col in colunas_alvo:
        if col not in df_principal.columns:
            df_principal[col] = None

    # 3. FILTRO ÚNICO: Tem Latitude e Longitude válidas?
    # Convertendo para numérico para evitar erros com strings vazias
    lat_valida = pd.to_numeric(df_principal['latitude_geocode'], errors='coerce').notna()
    lon_valida = pd.to_numeric(df_principal['longitude_geocode'], errors='coerce').notna()
    
    # Seleciona TUDO que tem coordenada
    df_para_processar = df_principal[lat_valida & lon_valida].copy()
    
    qtd = len(df_para_processar)
    if qtd == 0:
        print("⚠️ Nenhuma linha com coordenadas (Lat/Lon) encontrada para processar.")
        return df_principal

    print(f"🚀 Iniciando ATUALIZAÇÃO TOTAL para {qtd} linhas com coordenadas.")
    print("   Isso pode demorar um pouco dependendo da quantidade...")

    # 4. Processa (Consulta GeoServer para todas essas linhas)
    df_processado = enriquecer_candidatos_geoserver(df_para_processar)

    # 5. Atualiza a tabela original
    # O .update() vai sobrescrever os valores antigos pelos novos retornados pelo GeoServer
    df_principal.update(df_processado)

    print("✅ Atualização completa finalizada! Todos os campos (incluindo Classificacao) foram renovados.")
    return df_principal
#caminho_arquivo = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\dados_cet_pre_tratados.xlsx"

caminho_arquivo = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\Arquivos_de_teste\ARQUIVOS_DE_HOMOLOGACAO\DADOS_ENRIQUECIDOS\sinistros_infosiga_2026-01-19_11-17_enriquecido.xlsx"

# ==========================================================

# Verificação de segurança para garantir que o Python achou o arquivo
if not os.path.exists(caminho_arquivo):
    raise FileNotFoundError(f"❌ O arquivo não foi encontrado neste caminho: {caminho_arquivo}")
else:
    print(f"✅ Arquivo encontrado no servidor!")

# Carrega o arquivo (detecta se é CSV ou Excel pela extensão)
if caminho_arquivo.endswith('.csv'):
    df_meus_dados = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8') # Ajuste o sep se necessário
elif caminho_arquivo.endswith(('.xls', '.xlsx')):
    df_meus_dados = pd.read_excel(caminho_arquivo)
else:
    raise ValueError("Formato de arquivo não suportado (use .csv ou .xlsx)")

print(f"Total de linhas carregadas: {len(df_meus_dados)}")

# Checagem rápida
check = df_meus_dados[
    df_meus_dados['latitude_geocode'].notna() & 
    df_meus_dados['GET'].isna()
]
print(f"Linhas prontas para processar: {len(check)}")
# Roda a atualização em TODAS as linhas com coordenadas
df_atualizado = atualizar_tudo_geoserver(df_meus_dados)
# Verifica o status final
cols_check = ['GET', 'DET', 'SUB', 'Distrito_Nome', 'Regiao_Nome']

# Filtra o que tem Lat/Lon mas ainda falta algo
restantes = df_atualizado[
    df_atualizado['latitude_geocode'].notna() & 
    (df_atualizado[cols_check].isna().any(axis=1) | df_atualizado[cols_check].eq('').any(axis=1))
]

print(f"Resumo final: {len(restantes)} linhas permanecem incompletas após tentar atualizar.")

if len(restantes) > 0:
    print("\nExemplo das linhas que o GeoServer não retornou dados:")
    print(restantes[['latitude_geocode', 'longitude_geocode'] + cols_check].head())
# ==============================================================================
#  VERIFICAÇÃO DOS DADOS ENRIQUECIDOS
# ==============================================================================

cols_foco = ['DET', 'GET', 'SUB', 'Distrito_Nome', 'Regiao_Nome', 'Classificacao']

print("🔍 INSPEÇÃO FINAL DOS DADOS:")
print("-" * 50)

# 1. Checa vazios
vazios = df_atualizado[cols_foco].isna().sum()
print("Quantidade de linhas vazias por coluna:")
print(vazios)
print("-" * 50)

# 2. Amostra visual
print("Amostra de 5 linhas preenchidas:")
amostra = df_atualizado[df_atualizado['latitude_geocode'].notna()].sample(5)
print(amostra[cols_foco])

# 3. Resumo da Classificação
print("-" * 50)
print("Tipos de Classificação Viária encontrados:")
print(df_atualizado['Classificacao'].value_counts().head(10))

# ==============================================================================
#  SALVAR ARQUIVO
# ==============================================================================
nome_base = os.path.splitext(caminho_arquivo)[0]
extensao = os.path.splitext(caminho_arquivo)[1]
caminho_saida = f"{nome_base}_ENRIQUECIDO_FULL{extensao}"

print(f"\n💾 Salvando arquivo completo em: {caminho_saida}")

if extensao == '.csv':
    df_atualizado.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8')
else:
    df_atualizado.to_excel(caminho_saida, index=False)

print("✅ Arquivo salvo com sucesso!")

# ==============================================================================
#  ⏱️ CÁLCULO DO TEMPO TOTAL
# ==============================================================================
fim_processo = time.time()
tempo_total_segundos = fim_processo - inicio_processo

# Formata para HH:MM:SS
tempo_formatado = str(datetime.timedelta(seconds=int(tempo_total_segundos)))

print("\n" + "="*40)
print(f"🏁 PROCESSO FINALIZADO EM: {tempo_formatado}")
print("="*40)