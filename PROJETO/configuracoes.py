"""
Configurações centralizadas para o sistema de busca de endereços
"""

# ========== CONFIGURAÇÕES DE REDE ==========
GEOCODE_ENDERECOS_URL = "http://cetaplica/geosoap/geocode.asmx/buscaEnderecos"
GEOCODE_LATLON_URL = "http://cetaplica/geosoap/geocode.asmx"
GEOSERVER_WMS_URL = "http://cet-inf7242:8080/geoserver/sqlProducao/wms"

# ========== CONFIGURAÇÕES DE ARQUIVOS ==========
CADASTRO_PATH = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\CÓDIGOS_EM_USO\Projeto_Padronizacao_Logradouros\ARQUIVOS_DE_APOIO\cadastroRuas_V5_13_10_2025.xlsx"
SHAPEFILE_SP_PATH = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\CÓDIGOS_EM_USO\Projeto_Padronizacao_Logradouros\ARQUIVOS_DE_APOIO\vwBlCET_det_get_distrito_sub_4326\LIMITE_MUNICIPAL_4674.shp"

# ========== TERMOS BASE - ÚNICA LISTA ==========
_TERMOS_BASE = {
    "EDISON", "RUA EDISON", 
    "SPEERS", "SPEER", "ESTR", "PTE",
    "VILLA", "VILLA-LOBOS"
}

# ========== PREPOSIÇÕES E CONTEXTOS ==========
PREPOSICOES_INICIAIS = ["DO ", "DA ", "DE ", "DOS ", "DAS "]
CONTEXTOS_VALIDOS_NORMALIZACAO = [
    ' DE ', 'TUNEL', 'PONTE', 'VIADUTO', 'RUA', 'AVENIDA', 'ALAMEDA',
    'PRACA', 'LARGO', 'TRAVESSA', 'BECO', 'VIA', 'ELEVADO', 'PARQUE'
]

# ========== CONFIGURAÇÕES DE TIPOS DE VIA ==========
TIPOS_VIA = {
    "ACESSO", "ALAMEDA", "AVENIDA", "BECO", "CAMINHO", "COMPLEXO VIARIO", 
    "ESPACO LIVRE", "ESPLANADA", "ESTRADA", "ESCADARIA", "ESTRADA PARTICULAR", 
    "GALERIA", "LADEIRA", "LARGO", "PASSARELA", "PRACA", "PRACA PROJETADA", 
    "PARQUE", "PARQUE ESTADUAL", "PARQUE LINEAR", "PARQUE MUNICIPAL",
    "PASSAGEM DE PEDESTRE", "PASSAGEM PARTICULAR", "PASSAGEM SUBTERRÂNEA", 
    "PONTILHAO", "RUA", "RUA PARTICULAR", "RUA PROJETADA", "RODOVIA", 
    "TRAVESSA", "TRAVESSA PARTICULAR", "VIA DE CIRCULACAO DE PEDESTRES", 
    "VIADUTO", "VIELA", "VIA ELEVADA", "VIA ELEVADA DE PEDESTRES", "VEREDA", 
    "VIELA SANITARIA", "VILA", "VIELA PROJETADA", "VIELA PARTICULAR",
    # Abreviações
    "AC", "AL", "AV", "BC", "CM", "CV", "EL", "EPL", "ES", "ESC", "ESP", "GL", 
    "LD", "LG", "PA", "PC", "PP", "PQ", "PQE", "PQL", "PQM", "PS", "PSP", "PSS", 
    "PTL", "R", "RP", "RPJ", "RV", "TV", "TVP", "VCP", "VD", "VE", "VEL", "VEP", 
    "VER", "VES", "VL", "VLP", "VP", "DE", "DA", "DO"
}

# ✅ CONSOLIDADO - Tipos de rodovia (elimina tipos_rodovia duplicado)
TIPOS_RODOVIA = {"RV", "VIA", "RODOVIA", "RODOANEL"}

# ✅ CONSOLIDADO - Equivalências de tipos (elimina equivalencias_tipos duplicado)
EQUIVALENCIAS_TIPOS = {
    "R": "RUA", "RUA": "R",
    "AV": "AVENIDA", "AVENIDA": "AV", 
    "AL": "ALAMEDA", "ALAMEDA": "AL",
    "TV": "TRAVESSA", "TRAVESSA": "TV",
    "PR": "PRACA", "PRACA": "PR",
    "ES": "ESPLANADA", "ESPLANADA": "ES"
}

# ========== CONFIGURAÇÕES DE RODOVIAS ==========
MAPEAMENTO_RODOVIAS = {
    "SP 010": "07061", "BR 381": "07061", "SP 021": "51501", 
    "SP 060": "27099", "BR 116": "27099", "SP 070": "AYRTON SENNA", 
    "SP 150": "01247", "SP 160": "24491", "SP 015": "MARGINAL", 
    "SP 270": "16878", "SP 280": "13026", "SP 330": "01429", 
    "SP 348": "35449",
}

MARGINAIS = {
    "TIETE": ["33234", "15188", "02421", "06371", "14270", "12436", "04561"],
    "PINHEIROS": ["03376", "00569", "12502", "13014", "51879", "35858", "06238", "08889"],
    "OUTRAS_MARGINAIS": ["09863", "09716", "20961", "31844", "42621", "39620", "33509", "44616"],
    "RADIAL LESTE": ["00543", "00544", "01702", "07645", "12156", "22555", "34122", "35652", "33987","33667", "42308"],
    "AYRTON SENNA": ["46726", "49244", "Z1835", "Z1579", "Z4712", "Z1834", "Z2930", "Z4744", "Z1833", "42369", "Z4545", "Z3673"]
}

RODOVIAS_ESPECIAIS = {
    "AYRTON SENNA": MARGINAIS["AYRTON SENNA"],
    "MARGINAL": MARGINAIS["TIETE"] + MARGINAIS["PINHEIROS"] + MARGINAIS["OUTRAS_MARGINAIS"]
}

# ✅ CONSOLIDADO - Nomes de rodovias (elimina nomes_rodovias duplicado)
NOMES_RODOVIAS = {
    "AYRTON SENNA", "ANHANGUERA", "ANCHIETA", "IMIGRANTES",
    "FERNÃO DIAS", "BANDEIRANTES", "RAPOSO TAVARES", 
    "PRESIDENTE DUTRA", "RODOANEL", "MARIO COVAS",
    "MARGINAL TIETE", "MARGINAL PINHEIROS", "RADIAL LESTE"
}

# ✅ CONSOLIDADO - Rodovias com KM (elimina rodovias_com_km duplicado)
RODOVIAS_COM_KM = {
    # SP-070 - Ayrton Senna (apenas RVs)
    "RV DOS TRABALHADORES": (0.0, 18.5),
    "RV AYRTON SENNA": (0.0, 18.5),
    
    # SP-021 - Rodoanel (apenas RVs)
    "RV MARIO COVAS": [(0.0, 9.3), (47.96, 66.68), (84.0, 103.0)],
    "RODOANEL MARIO COVAS": [(0.0, 9.3), (47.96, 66.68), (84.0, 103.0)],
    
    # SP-060 - Presidente Dutra (apenas RVs)
    "RV PRES DUTRA": (227.2, 232.0),
    
    # SP-330 - Anhanguera (apenas VIA)
    "VIA ANHANGUERA": (0.0, 29.5),
    
    # SP-270 - Raposo Tavares (apenas VIA)
    "VIA RAPOSO TAVARES": (9.8, 20.2),
    
    # SP-010 - Fernão Dias (apenas RODOVIA)
    "RODOVIA FERNÃO DIAS": (73.0, 88.2),
    
    # SP-150 - Anchieta (apenas RODOVIA)
    "RODOVIA ANCHIETA": (0.0, 13.0),
    
    # SP-160 - Imigrantes (apenas RODOVIA)
    "RODOVIA IMIGRANTES": (0.0, 13.5),
    
    # SP-348 - Bandeirantes (apenas RODOVIA)
    "RODOVIA BANDEIRANTES": (0.0, 28.5),
}

# ========== TERMOS ESPECIAIS ==========
TERMOS_ESTRUTURA = {"PONTE", "VIADUTO", "TUNEL", "PASSARELA", "TRAVESSIA"}
TERMOS_RODOVIA = NOMES_RODOVIAS  # ✅ CONSOLIDADO - usa NOMES_RODOVIAS

# ========== CONFIGURAÇÕES DE BUSCA ==========
MAX_TENTATIVAS_REQUISICAO = 3
TIMEOUT_REQUISICAO = 10
MAX_CANDIDATOS_PADRAO = 35
MIN_SIMILARIDADE_EMBEDDING = 60
MIN_SIMILARIDADE_EMBEDDING_ESTRUTURAS = 50
TOP_K_EMBEDDING_TERMOS_CURTOS = 25
TOP_K_EMBEDDING_TERMOS_LONGOS = 20

# ========== PADRÕES REGEX ==========
PADROES_RODOVIA = [
    r'\b(SP|BR)\s*\d{2,3}\b',
    r'\bSP\s*0?-?\s*15\b',
    r'\bSP\s*0?-?\s*070\b',
    r'\bMARGINAL\b',
    r'\bRODOVIA\b',
    r'\bAYRTON\s*SENNA\b',
    r'\bRADIAL\s*LESTE\b'
]

# ========== CONFIGURAÇÕES DE DETECÇÃO DE VIAS ESPECIAIS ==========
RODOVIAS_POR_NOME = {
    "AYRTON SENNA": "42369",
    "ANHANGUERA": "01429", 
    "ANCHIETA": "01247",
    "IMIGRANTES": "24491",
    "FERNÃO DIAS": "07061",
    "BANDEIRANTES": "35449",
    "RAPOSO TAVARES": "16878",
    "PRESIDENTE DUTRA": "27099",
    "RODOANEL": "51501",
    "MARIO COVAS": "51501"
}

DETECTOR_RODOVIAS_ESPECIAIS = {
    "AYRTON SENNA": ["SP 070", "RODOVIA AYRTON SENNA"],
    "MARGINAL": ["SP 015", "MARGINAL TIETE", "MARGINAL PINHEIROS"],
    "RADIAL LESTE": ["RADIAL LESTE"]
}

TIPOS_VIA_URBANA = {
    "COMPLEXO", "COMPLEXO VIARIO", "CV", "ACESSO", "AC", 
    "RUA", "AVENIDA", "ALAMEDA", "TRAVESSA", "VIELA", "BECO", 
    "R", "AV", "TV"
}

# ========== ORDEM DE PRIORIDADE DAS FONTES DE BUSCA ==========
ORDEM_PRIORIDADE_FONTES = {
    "EXATO": 0, 
    "EXATO_SEM_TIPO": 1, 
    "RELATIVO": 2, 
    "EMBEDDING": 3, 
    "VIAS_COMPLEXAS": 4, 
    "RODOVIA": 5
}

# ========== CONFIGURAÇÕES DE EMBEDDING ==========
CONFIG_EMBEDDING = {
    "TERMOS_CURTOS": {
        "top_k": 25,
        "min_sim": 55
    },
    "TERMOS_LONGOS": {
        "top_k": 20, 
        "min_sim": 60
    }
}