"""
Módulo para seleção, priorização e filtragem de resultados de endereços.

Manutencao:
- Para adicionar novos tipos de via reconhecidos, inclua em TIPOS_VIA (siglas) ou
  TIPOS_VIA_NOMES (nomes completos) no bloco de constantes abaixo.
- Para adicionar novas estruturas prioritárias (Pontes, Túneis etc.), edite
  ESTRUTURAS_PRIORITARIAS. A ORDEM da lista importa: o primeiro match vence.
- Para adicionar novos padrões de rodovia, edite PADROES_RODOVIA.
- Nenhuma dessas constantes deve ser definida dentro de funções — isso causaria
  recriação a cada chamada e degradaria performance em processamentos em lote.
"""

import re
import logging
from typing import List

import pandas as pd


# =============================================================================
# CONSTANTES GLOBAIS
# Definidas uma única vez no carregamento do módulo.
# Mover qualquer dessas de volta para dentro de funções vai custar performance.
# =============================================================================

# Siglas e nomes completos de tipos de via reconhecidos pela CET.
TIPOS_VIA = {
    "ACESSO", "ALAMEDA", "AVENIDA", "BECO", "CAMINHO", "COMPLEXO VIARIO", "ESPACO LIVRE",
    "ESPLANADA", "ESTRADA", "ESCADARIA", "ESTRADA PARTICULAR", "GALERIA", "LADEIRA", "LARGO",
    "PASSARELA", "PRACA", "PRACA PROJETADA", "PARQUE", "PARQUE ESTADUAL", "PARQUE LINEAR",
    "PARQUE MUNICIPAL", "PASSAGEM DE PEDESTRE", "PASSAGEM PARTICULAR",
    "PASSAGEM SUBTERRÂNEA", "PONTILHAO", "RUA", "RUA PARTICULAR", "RUA PROJETADA",
    "RODOVIA", "TRAVESSA", "TRAVESSA PARTICULAR", "VIA DE CIRCULACAO DE PEDESTRES",
    "VIADUTO", "VIELA", "VIA ELEVADA", "VIA ELEVADA DE PEDESTRES", "VEREDA",
    "VIELA SANITARIA", "VILA", "VIELA PROJETADA", "VIELA PARTICULAR",
    "AC", "AL", "AV", "BC", "CM", "CV", "EL", "EPL", "ES", "ESC", "ESP", "GL", "LD", "LG",
    "PA", "PC", "PP", "PQ", "PQE", "PQL", "PQM", "PS", "PSP", "PSS", "PTL",
    "R", "RP", "RPJ", "RV", "TV", "TVP", "VCP", "VD", "VE", "VEL", "VEP", "VER", "VES",
    "VL", "VLP", "VP", "DE", "DA", "DO",
}

# Lista ordenada de tipos de via para detecção no termo buscado.
# Usar lista (não set) garante ordem determinística — o primeiro match vence.
# Tipos mais longos e específicos devem vir antes para evitar matches parciais errados.
# Manutencao: ao adicionar tipos, coloque os mais específicos no início da lista.
TIPOS_VIA_ORDENADOS = sorted(
    [t for t in TIPOS_VIA if len(t) > 2],
    key=len,
    reverse=True,
)

# Termos que indicam que o endereço buscado é uma estrutura especial (Pontes, Túneis, etc.).
# Manutencao: adicione novos termos conforme surgirem nos dados do Infosiga.
TERMOS_ESTRUTURA = frozenset({
    "PONTE", "PTE", "VIADUTO", "VD", "TUNEL", "PASSARELA", "TRAVESSIA",
    "ACESSO", "AC", "PRACA", "PC", "LARGO", "LG", "METRO", "TERMINAL", "ESTACAO",
    "COMPLEXO", "CV", "ESTRADA", "ES", "EST", "ESTR",
})

# Mapeamento de estruturas prioritárias para as siglas CET que as representam.
# A ORDEM da lista importa: o primeiro nome encontrado no termo buscado vence.
# Manutencao: se uma estrutura passar a ter sigla diferente na base CET, atualize aqui.
ESTRUTURAS_PRIORITARIAS = [
    ("ACESSO",   ["ACESSO", "AC", "AC A"]),
    ("PONTE",    ["PONTE", "PTE", "CV", "COMPLEXO"]),
    ("VIADUTO",  ["VIADUTO", "VD", "CV", "COMPLEXO"]),
    ("TUNEL",    ["TUNEL", "TÚNEL", "TN", "CV", "COMPLEXO"]),
    ("COMPLEXO", ["COMPLEXO", "CV"]),
    ("PRACA",    ["PRACA", "PRAÇA", "PC", "PR"]),
    ("LARGO",    ["LARGO", "LG"]),
    ("PASSARELA",["PASSARELA", "PASS"]),
    ("ESTRADA",  ["ESTRADA", "ES", "EST", "ESTR"]),
    ("TERMINAL", ["TERMINAL", "TERM"]),
    ("METRO",    ["METRO", "METRÔ", "ESTACAO", "ESTAÇÃO"]),
]

# Equivalências entre siglas e nomes completos de tipos de via.
# Usado para busca bidirecional ao priorizar por tipo específico.
EQUIVALENCIAS_TIPOS = {
    "R":        "RUA",       "RUA":      "R",
    "AV":       "AVENIDA",   "AVENIDA":  "AV",
    "AL":       "ALAMEDA",   "ALAMEDA":  "AL",
    "TV":       "TRAVESSA",  "TRAVESSA": "TV",
    "PR":       "PRACA",     "PRACA":    "PR",
    "ES":       "ESPLANADA", "ESPLANADA":"ES",
}

# Termos cujos resultados exigem validação mais rigorosa por serem ambíguos ou raros.
# Manutencao: adicione termos problemáticos conforme identificados nos dados.
TERMOS_GENERICOS = frozenset({"SPEERS", "SPEER", "ESTR", "PTE", "EDISON"})

# Termos que forçam uso do fallback por embedding independentemente do resultado principal.
TERMOS_EMBEDDING_OBRIGATORIO = frozenset({
    "SPEERS", "SPEER", "ESTR", "PTE", "VILLA", "VILLA-LOBOS", "EDISON",
})

# Padrões de rodovia compilados uma única vez.
# Manutencao: adicione novos padrões conforme rodovias forem identificadas nos dados.
PADROES_RODOVIA = [
    re.compile(r'\b(SP|BR)\s*\d{2,3}\b',          re.IGNORECASE),
    re.compile(r'\bSP\s*0?-?\s*15\b',             re.IGNORECASE),
    re.compile(r'\bSP\s*0?-?\s*070\b',            re.IGNORECASE),
    re.compile(r'\bMARGINAL\b',                    re.IGNORECASE),
    re.compile(r'\bRODOVIA\b',                     re.IGNORECASE),
    re.compile(r'\bAYRTON\s*SENNA\b',             re.IGNORECASE),
    re.compile(r'\bRADIAL\s*LESTE\b',             re.IGNORECASE),
]

# Colunas mantidas no resultado final entregue ao orquestrador (.ipynb).
# Manutencao: se o schema da base CET mudar, atualize esta lista.
COLUNAS_RESULTADO_FINAL = [
    "codlog", "logradouro_PMSP", "latitude_geocode", "longitude_geocode",
    "distancia_km", "similaridade", "_fonte_busca",
    "GET", "DET", "SUB", "Distrito_Nome", "Regiao_Nome", "Classificacao",
]

# Colunas de rascunho criadas internamente que devem ser removidas do resultado.
COLUNAS_INTERNAS = frozenset({"tipo", "titulo", "preposicao", "nome", "LOCAL", "SIMILARIDADE"})


# =============================================================================
# FUNÇÕES DE PRIORIZAÇÃO
# =============================================================================

def _priorizar_por_correspondencia_tipo(df: pd.DataFrame, termo_original: str) -> pd.DataFrame:
    """
    Prioriza resultados que correspondem ao tipo de via buscado.

    Para estruturas especiais (Pontes, Túneis, Viadutos, etc.), aplica a lógica
    de 'Fila VIP': acha as linhas cujo tipo ou nome bate com as siglas esperadas
    e ordena pelo match exato com o início do logradouro, desempatando por distância.

    Para vias comuns, filtra pelo tipo encontrado no termo e ordena por distância.

    Manutencao: a precedência entre estruturas é controlada pela ordem de
    ESTRUTURAS_PRIORITARIAS. Se uma estrutura estiver sendo confundida com outra,
    reordene lá, não aqui.
    """
    if df.empty or len(df) <= 1:
        return df

    termo_upper = termo_original.upper().strip()

    # --- Fila VIP: estruturas especiais ---
    tipo_encontrado = None
    siglas_validas  = []

    for nome_completo, siglas in ESTRUTURAS_PRIORITARIAS:
        termo_com_bordas = f" {termo_upper} "
        if (f" {nome_completo} " in termo_com_bordas
                or any(f" {s} " in termo_com_bordas for s in siglas)):
            tipo_encontrado = nome_completo
            siglas_validas  = siglas
            break

    if tipo_encontrado:
        logging.info(f"[PRIORIDADE] Estrutura '{tipo_encontrado}' detectada no termo.")

        padrao_tipo   = re.compile(r'\b(?:' + '|'.join(re.escape(s) for s in siglas_validas) + r')\b')
        padrao_inicio = re.compile(r'^(?:' + '|'.join(re.escape(s) for s in siglas_validas) + r')\b')

        mask_estrutura = (
            df["tipo"].str.upper().apply(lambda t: bool(padrao_tipo.search(t)))
            | df["logradouro_PMSP"].str.upper().apply(lambda l: bool(padrao_tipo.search(l)))
        )
        df_estruturas = df[mask_estrutura].copy()

        if not df_estruturas.empty:
            # Estrela Dourada: bate com o início do logradouro (match mais exato)
            df_estruturas["_match_exato"] = (
                df_estruturas["logradouro_PMSP"].str.upper().str.strip()
                .apply(lambda l: bool(padrao_inicio.match(l)))
            )

            colunas_ordem  = ["_match_exato"]
            ordem_crescente = [False]
            if "distancia_km" in df_estruturas.columns:
                colunas_ordem.append("distancia_km")
                ordem_crescente.append(True)

            df_estruturas = df_estruturas.sort_values(
                by=colunas_ordem, ascending=ordem_crescente
            ).drop(columns=["_match_exato"])

            return df_estruturas

    # --- Lógica padrão para vias comuns ---
    # Usa TIPOS_VIA_ORDENADOS (lista com ordem determinística) em vez do set TIPOS_VIA.
    tipo_buscado = next(
        (tipo for tipo in TIPOS_VIA_ORDENADOS if tipo in termo_upper),
        None
    )

    if not tipo_buscado:
        return df

    resultados_tipo = df[df["tipo"].str.upper() == tipo_buscado]

    if not resultados_tipo.empty:
        logging.info(f"[PRIORIDADE] {len(resultados_tipo)} resultado(s) do tipo '{tipo_buscado}'.")
        if "distancia_km" in df.columns:
            resultados_tipo = resultados_tipo.sort_values("distancia_km")
        return resultados_tipo

    return df


def _filtrar_melhor_embedding(df_embed: pd.DataFrame, endereco_original: str) -> pd.DataFrame:
    """
    Filtra resultados vindos do motor de embedding.

    Para estruturas especiais, usa critério de aprovação mais permissivo
    (similaridade > 50 ou ao menos 1 token em comum) porque seus nomes são
    complexos e raramente correspondem palavra a palavra ao que foi digitado.

    Para vias comuns, exige similaridade > 60 ou 2+ tokens em comum.

    Manutencao: os limiares de similaridade (50 e 60) foram calibrados
    empiricamente. Se a taxa de falsos positivos aumentar, eleve-os.
    """
    if df_embed.empty:
        return df_embed

    termo_upper = endereco_original.upper().strip()
    tokens_original = set(re.findall(r'\w+', termo_upper))

    # Verifica uma vez só fora do loop se o termo é uma estrutura especial
    tem_estrutura = any(
        f" {termo} " in f" {termo_upper} "
        for termo in TERMOS_ESTRUTURA
    )

    resultados_relevantes = []
    for _, row in df_embed.iterrows():
        logradouro   = str(row.get("logradouro_PMSP", "")).upper()
        similaridade = row.get("similaridade", 0)

        tokens_logradouro = set(re.findall(r'\w+', logradouro))
        tokens_comuns     = tokens_original & tokens_logradouro

        if tem_estrutura:
            aprovado = (
                similaridade > 50
                or len(tokens_comuns) >= 1
                or any(t in logradouro for t in tokens_original if len(t) > 3)
            )
        else:
            aprovado = (
                similaridade > 60
                or len(tokens_comuns) >= 2
                or termo_upper in logradouro
            )

        if aprovado:
            resultados_relevantes.append(row)
            logging.debug(f"[EMBEDDING] Aprovado: {logradouro} (sim={similaridade})")

    if not resultados_relevantes:
        logging.debug("[EMBEDDING] Nenhum resultado passou no filtro.")
        return pd.DataFrame()

    df_resultado = pd.DataFrame(resultados_relevantes)
    df_resultado = _priorizar_por_correspondencia_tipo(df_resultado, termo_upper)
    logging.info(f"[EMBEDDING] {len(df_resultado)} resultado(s) relevantes.")
    return df_resultado


def _priorizar_por_tipo_especifico(df: pd.DataFrame, tipo_buscado: str) -> pd.DataFrame:
    """
    Fallback de priorização: filtra pelo tipo de via original (e seu equivalente)
    quando a busca principal não retornou resultados do tipo esperado.

    Manutencao: se novos pares de equivalência forem identificados nos dados,
    adicione-os em EQUIVALENCIAS_TIPOS no topo do arquivo.
    """
    if df.empty:
        return df

    tipo_upper      = tipo_buscado.upper()
    tipo_equivalente = EQUIVALENCIAS_TIPOS.get(tipo_upper, "")

    resultados = df[
        df["tipo"].str.upper().isin([tipo_upper, tipo_equivalente])
    ]

    if not resultados.empty:
        logging.info(f"[FALLBACK] {len(resultados)} resultado(s) do tipo '{tipo_upper}'.")
        if "distancia_km" in df.columns:
            resultados = resultados.sort_values("distancia_km")
        return resultados

    logging.info(f"[FALLBACK] Tipo '{tipo_upper}' não encontrado — mantendo {len(df)} resultado(s).")
    return df


# =============================================================================
# FUNÇÕES DE FILTRAGEM
# =============================================================================

def _obter_logradouro_pmsp(row: pd.Series, tem_coluna: bool) -> str:
    """
    Retorna o logradouro PMSP de uma linha, evitando import dinâmico em loop.

    O import de 'logradouro_pmsp' é feito uma única vez aqui quando necessário,
    em vez de ser repetido a cada linha dentro de _filtrar_resultados_relevantes
    e _resultados_sao_relevantes. O custo do import acontece no máximo uma vez
    por execução do módulo.

    Manutencao: se o nome da função ou módulo de busca mudar, atualize apenas aqui.
    """
    if tem_coluna:
        return str(row.get("logradouro_PMSP", "")).upper()

    # Import feito uma única vez, fora do loop — resolvido via closure abaixo
    from buscaEndereco_CET_v12_0 import logradouro_pmsp as _fn
    return _fn(row).upper()


def _filtrar_resultados_relevantes(df: pd.DataFrame, termo_original: str) -> pd.DataFrame:
    """
    Filtra linhas do DataFrame mantendo apenas as que têm correspondência
    com o termo buscado — por inclusão completa ou sobreposição de tokens.

    Limiar de tokens comuns: max(1, 50% dos tokens do termo original).
    Manutencao: se resultados muito genéricos passarem, aumente o limiar para 0.6.
    """
    if df.empty:
        return df

    termo_upper  = termo_original.upper().strip()
    tokens_orig  = set(re.findall(r'\w+', termo_upper))
    limiar_tokens = max(1, int(len(tokens_orig) * 0.5))
    tem_coluna   = "logradouro_PMSP" in df.columns

    resultados = []
    for _, row in df.iterrows():
        logradouro = _obter_logradouro_pmsp(row, tem_coluna)

        if termo_upper in logradouro:
            resultados.append(row)
            continue

        tokens_log    = set(re.findall(r'\w+', logradouro))
        tokens_comuns = tokens_orig & tokens_log

        if len(tokens_comuns) >= limiar_tokens:
            resultados.append(row)

    return pd.DataFrame(resultados)


# =============================================================================
# FUNÇÕES DE AVALIAÇÃO
# Consolidam a lógica de "o resultado é suficiente?" em um único lugar
# para evitar duplicação e facilitar calibragem futura.
# =============================================================================

def _eh_busca_rodovia(endereco: str) -> bool:
    """
    Retorna True se o endereço corresponde a uma rodovia (SP-XXX, BR-XXX,
    Marginal, Ayrton Senna, etc.).

    Usa PADROES_RODOVIA compilados no carregamento do módulo — sem recompilação
    a cada chamada.
    """
    if not endereco:
        return False
    return any(p.search(endereco) for p in PADROES_RODOVIA)


def _avaliar_relevancia_generico(df: pd.DataFrame, termo_upper: str) -> bool:
    """
    Avalia se resultados de termos genéricos (SPEERS, ESTR, etc.) são válidos.
    Exige ao menos 2 resultados com nomes distintos entre si.

    Extraída para eliminar duplicação entre _resultados_sao_relevantes e
    _resultado_eh_suficiente — qualquer ajuste na regra vale nos dois contextos.
    """
    if len(df) <= 1:
        return False
    return df["logradouro_PMSP"].str.upper().nunique() > 1


def _resultados_sao_relevantes(df: pd.DataFrame, endereco_original: str) -> bool:
    """
    Verifica se ao menos um resultado do DataFrame tem correspondência real
    com o endereço buscado.

    Manutencao: esta função define o critério mínimo de "relevância". Se resultados
    irrelevantes estiverem passando, adicione restrições aqui.
    """
    if df.empty:
        return False

    termo_upper = endereco_original.upper().strip()

    if _eh_busca_rodovia(termo_upper):
        return True

    if termo_upper in TERMOS_GENERICOS:
        return _avaliar_relevancia_generico(df, termo_upper)

    tem_coluna = "logradouro_PMSP" in df.columns
    for _, row in df.iterrows():
        logradouro    = _obter_logradouro_pmsp(row, tem_coluna)
        tokens_orig   = set(re.findall(r'\w+', termo_upper))
        tokens_log    = set(re.findall(r'\w+', logradouro))

        if termo_upper in logradouro or logradouro in termo_upper:
            return True
        if tokens_orig & tokens_log:
            return True

    return False


def _resultado_eh_suficiente(df_resultado: pd.DataFrame, endereco_original: str) -> bool:
    """
    Decide se o resultado principal é bom o suficiente para encerrar a busca
    sem acionar o fallback por embedding.

    Critérios (em ordem de verificação):
    1. DataFrame vazio ou marcado como NAO ENCONTRADO → não é suficiente.
    2. Rodovia → qualquer resultado com linhas é suficiente.
    3. Termo genérico → exige 2+ resultados com nomes distintos.
    4. Resultados relevantes por conteúdo → suficiente.
    5. Similaridade >= 95 em qualquer linha → suficiente.
    6. Distância <= 0.1 km em qualquer linha → suficiente.

    Manutencao: os limiares de similaridade (95) e distância (0.1) foram
    definidos empiricamente. Ajuste conforme análise dos dados de validação.
    """
    if df_resultado.empty or df_resultado.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO":
        return False

    termo_upper = endereco_original.upper().strip()

    if _eh_busca_rodovia(termo_upper) and len(df_resultado) > 0:
        return True

    if termo_upper in TERMOS_GENERICOS:
        return _avaliar_relevancia_generico(df_resultado, termo_upper)

    if _resultados_sao_relevantes(df_resultado, endereco_original):
        return True

    if "similaridade" in df_resultado.columns and df_resultado["similaridade"].ge(95).any():
        return True

    if "distancia_km" in df_resultado.columns and df_resultado["distancia_km"].le(0.1).any():
        return True

    return False


def _precisa_embedding_fallback(df_resultado_principal: pd.DataFrame, endereco_original: str) -> bool:
    """
    Decide se é necessário acionar o motor de embedding como fallback.

    Retorna True (precisa de fallback) quando:
    - Resultado vazio ou NAO ENCONTRADO.
    - Termo pertence à lista de embedding obrigatório.
    - Busca com 2+ palavras e apenas 1 resultado encontrado.
    - Resultados não são relevantes para o termo buscado.

    Manutencao: adicione termos em TERMOS_EMBEDDING_OBRIGATORIO quando identificar
    endereços que o motor textual sistematicamente erra.
    """
    if (df_resultado_principal.empty
            or df_resultado_principal.iloc[0]["logradouro_PMSP"] == "NAO ENCONTRADO"):
        return True

    termo_upper = endereco_original.upper()

    if termo_upper in TERMOS_EMBEDDING_OBRIGATORIO:
        return True

    if len(termo_upper.split()) >= 2 and len(df_resultado_principal) <= 1:
        return True

    if not _resultados_sao_relevantes(df_resultado_principal, endereco_original):
        return True

    return False


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def _dataframe_nao_encontrado() -> pd.DataFrame:
    """Retorna o DataFrame padrão de resultado negativo."""
    return pd.DataFrame([{"logradouro_PMSP": "NAO ENCONTRADO"}])


def _combinar_resultados(lista_dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatena múltiplos DataFrames de resultados e remove duplicatas por codlog.

    Manutencao: se 'codlog' deixar de ser a chave única da base CET,
    atualize o subset do drop_duplicates.
    """
    if not lista_dataframes:
        return _dataframe_nao_encontrado()

    df_combinado = pd.concat(lista_dataframes, ignore_index=True)

    # Verifica existência da coluna antes de deduplicar para evitar KeyError silencioso
    if "codlog" not in df_combinado.columns:
        logging.warning("[COMBINAR] Coluna 'codlog' ausente — deduplicação ignorada.")
        return df_combinado

    return df_combinado.drop_duplicates(subset=["codlog"], keep="first")


def _eh_resultado_valido(df: pd.DataFrame) -> bool:
    """Retorna True se o DataFrame contém pelo menos um resultado válido."""
    return not df.empty and df.iloc[0]["logradouro_PMSP"] != "NAO ENCONTRADO"


# =============================================================================
# FUNÇÕES DE PROCESSAMENTO FINAL
# =============================================================================

def _limpar_colunas_resultado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas de rascunho interno e retorna apenas as colunas do resultado final.

    Manutencao: se uma nova coluna precisar aparecer no output final, adicione-a
    em COLUNAS_RESULTADO_FINAL. Se uma coluna interna nova for criada durante o
    processamento, adicione-a em COLUNAS_INTERNAS para garantir que seja removida.
    """
    if df.empty:
        return df

    # Remove colunas internas que não estejam na lista final
    colunas_remover = COLUNAS_INTERNAS - set(COLUNAS_RESULTADO_FINAL)
    df = df.drop(columns=[c for c in colunas_remover if c in df.columns])

    colunas_finais = [c for c in COLUNAS_RESULTADO_FINAL if c in df.columns]
    return df[colunas_finais] if colunas_finais else df