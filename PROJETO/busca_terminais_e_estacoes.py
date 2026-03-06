"""
Módulo especializado para detecção de terminais, estações e pontos de transporte público
Autor: CET
Versão: 1.0
"""

import re
import logging
import pandas as pd
from typing import Set, List

# ========== LISTAS COMPLETAS DE PONTOS DE TRANSPORTE ==========

TERMINAIS = [
    "TERMINAL A E CARVALHO", "TERMINAL AMARAL GURGEL", "TERMINAL ANA ROSA", 
    "TERMINAL ARICANDUVA", "TERMINAL ARTUR ALVIM", "TERMINAL BANDEIRA",
    "TERMINAL RODOVIARIO BARRA FUNDA", "TERMINAL BARRA FUNDA", "TERMINAL BELEM",
    "TERMINAL BUTANTA", "TERMINAL CAMPO LIMPO", "TERMINAL CAPELINHA",
    "TERMINAL CARRAO", "TERMINAL CASA VERDE", "TERMINAL CIDADE TIRADENTES",
    "TERMINAL CIDADE UNIVERSITARIA", "TERMINAL CORREIO", "TERMINAL CPTM JOSE BONIFACIO",
    "TERMINAL ESTACAO CPTM GUAIANAZES", "TERMINAL GRAJAU", "TERMINAL GUARAPIRANGA",
    "TERMINAL ITAQUERA", "TERMINAL RODOVIARIO JABAQUARA", "TERMINAL JABAQUARA",
    "TERMINAL JARDIM ANGELA", "TERMINAL JOAO DIAS", "TERMINAL LAPA", "TERMINAL MERCADO",
    "TERMINAL METALURGICOS", "TERMINAL METRO ANA ROSA", "TERMINAL METRO ARTUR ALVIM",
    "TERMINAL METRO BELEM", "TERMINAL METRO BUTANTA", "TERMINAL METRO CARRAO",
    "TERMINAL METRO ITAQUERA", "TERMINAL METRO PARADA INGLESA", "TERMINAL METRO PATRIARCA",
    "TERMINAL METRO PENHA", "TERMINAL METRO SANTANA", "TERMINAL METRO TATUAPE",
    "TERMINAL METRO TUCURUVI", "TERMINAL METRO VILA MADALENA", "TERMINAL METRO VILA MARIANA",
    "TERMINAL METRO VILA MATILDE", "TERMINAL PARADA INGLESA", "TERMINAL PARELHEIROS",
    "TERMINAL PARQUE D PEDRO", "TERMINAL PARQUE DOM PEDRO", "TERMINAL PARQUE DOM PEDRO II",
    "TERMINAL PATRIARCA", "TERMINAL PENHA", "TERMINAL PINHEIROS", "TERMINAL PIRITUBA",
    "TERMINAL PQ D PEDRO", "TERMINAL PQ DOM PEDRO", "TERMINAL PQ DOM PEDRO II",
    "TERMINAL PRINCESA ISABEL", "TERMINAL RODOVIARIO DO JACIRA", "TERMINAL TIETE",
    "TERMINAL RODOVIARIO TIETE", "TERMINAL S MATEUS", "TERMINAL S MIGUEL", "TERMINAL SACOMA",
    "TERMINAL SANTA CRUZ", "TERMINAL SANTANA", "TERMINAL SANTO AMARO", "TERMINAL SAO MIGUEL",
    "TERMINAL SAPOPEMBA", "TERMINAL SAPOPEMBA/TEOTONIO VILELA", "TERMINAL STO AMARO",
    "TERMINAL TATUAPE", "TERMINAL TEOTONIO VILELA", "TERMINAL TUCURUVI", "TERMINAL USP",
    "TERMINAL VARGINHA", "TERMINAL VILA CARRAO", "TERMINAL VILA IARA", "TERMINAL VILA MADALENA",
    "TERMINAL VILA MARIANA", "TERMINAL VILA MATILDE", "TERMINAL VILA NOVA CACHOEIRINHA"
]

# Bases de estações (serão preenchidas no __init__)
CPTM_BASE = [
    "GUARULHOS CECAP", "ITAPEVI", "JURUBATUBA", "OSASCO", "TATUAPE",
    "JARDIM HELENA - VILA MARA", "JARDIM ROMANO", "AGUA BRANCA",
    "ANTONIO GIANETTI NETO", "ANTONIO JOAO", "ARACARE", "BALTAZAR FIDELIS",
    "PALMEIRAS - BARRA FUNDA", "BARUERI", "BOTUJURU", "BRAS CUBAS",
    "CAIEIRAS", "CAMPO LIMPO PAULISTA", "CAPUAVA", "CARAPICUIBA", "CEASA",
    "CIDADE UNIVERSITARIA", "COMENDADOR ERMELINO", "COMANDANTE SAMPAIO",
    "DOMINGOS DE MORAIS", "ENGENHEIRO GOULART", "ENGENHEIRO MANOEL FEIO",
    "ENGENHEIRO CARDOSO", "ESTUDANTES", "FERRAZ DE VASCONCELOS",
    "FRANCISCO MORATO", "FRANCO DA ROCHA", "GUAPITUBA", "AEROPORTO GUARULHOS",
    "PARADA AMADOR BUENO", "BERRINI", "AUTODROMO", "CALMON VIANA", "PINHEIROS",
    "GENERAL MIGUEL COSTA", "JUNDIAI", "PRIMAVERA - INTERLAGOS", "GRAJAU",
    "PREFEITO CELSO DANIEL - SANTO ANDRE", "HEBRAICA - REBOUCAS", "CIDADE JARDIM",
    "VILA OLIMPIA", "MORUMBI", "GRANJA JULIETA", "SOCORRO", "CORINTHIANS - ITAQUERA",
    "DOM BOSCO", "JOSE BONIFACIO", "GUAIANAZES", "USP LESTE", "IMPERATRIZ LEOPOLDINA",
    "IPIRANGA", "ITAIM PAULISTA", "ITAQUAQUECETUBA", "JANDIRA", "JARAGUA",
    "JARDIM BELVAL", "JARDIM SILVEIRA", "JULIO PRESTES", "PRESIDENTE ALTINO",
    "QUITAUNA", "RIBEIRAO PIRES", "RIO GRANDE DA SERRA", "SAGRADO CORACAO",
    "SANTA TEREZINHA", "SAO CAETANO", "SAO MIGUEL PAULISTA", "SUZANO",
    "TAMANDUATEI", "UTINGA", "VARZEA PAULISTA", "VILA CLARICE", "BRAS",
    "VILLA-LOBOS - JAGUARE", "VILA AURORA", "ESTACAO SANTA RITA", "JUNDIAPEBA",
    "LAPA", "SANTO AMARO", "LUZ", "MAUA", "MOGI DAS CRUZES", "MOOCA", "PERUS",
    "PIQUERI", "PIRITUBA", "POA", "PREFEITO SALADINO", "JOAO DIAS", "MENDES-VILA NATAL"
]

METRO_BASE = [
    "AACD-SERVIDOR", "ADOLFO PINHEIRO", "ALTO DA BOA VISTA", "ALTO DO IPIRANGA",
    "ANA ROSA", "ANHANGABAU", "ARMÊNIA", "ARTUR ALVIM", "AYRTON SENNA-JARDIM SAO PAULO",
    "BELEM", "BORBA GATO", "BRAS", "BRESSER-MOOCA", "BRIGADEIRO", "BROOKLIN",
    "BUTANTA", "CAMILO HADDAD", "CAMPO BELO", "CAMPO LIMPO", "CAPAO REDONDO",
    "CARANDIRU", "CARRAO", "CHACARA KLABIN", "CLINICAS", "CONCEICAO", "CONSOLACAO",
    "CORINTHIANS-ITAQUERA", "EUCALIPTOS", "FARIA LIMA", "FAZENDA DA JUTA",
    "FRADIQUE COUTINHO", "GIOVANNI GRONCHI", "GUILHERMINA-ESPERANCA",
    "HIGIENOPOLIS-MACKENZIE", "HOSPITAL SAO PAULO", "JABAQUARA", "JAPAO-LIBERDADE",
    "JARDIM PLANALTO", "LARGO TREZE", "LUZ", "MARECHAL DEODORO", "MOEMA", "ORATORIO",
    "OSCAR FREIRE", "PALMEIRAS-BARRA FUNDA", "PARADA INGLESA", "PARAISO", "PATRIARCA",
    "PAULISTA", "PEDRO II", "PENHA", "PINHEIROS", "PORTUGUESA-TIETÊ", "PRACA DA ARVORE",
    "REPUBLICA", "SACOMA", "SANTA CECILIA", "SANTA CRUZ", "SANTANA", "SANTO AMARO",
    "SANTOS-IMIGRANTES", "SANTUARIO NOSSA SENHORA DE FATIMA-SUMARE", "SAO BENTO",
    "SAO JOAQUIM", "SAO JUDAS", "SAO LUCAS", "SAO MATEUS", "SAO PAULO-MORUMBI",
    "SAPOPEMBA", "SAUDE", "SE", "TAMANDUATEI", "TATUAPE", "TIRADENTES", "TRIANON-MASP",
    "TUCURUVI", "VERGUEIRO", "VILA DAS BELEZAS", "VILA MADALENA", "VILA MARIANA",
    "VILA MATILDE", "VILA PRUDENTE", "VILA SONIA", "VILA TOLSTOI", "VILA UNIAO"
]

VIAQUATRO_BASE = [
    "BUTANTA", "FARIA LIMA", "FRADIQUE COUTINHO", "HIGIENOPOLIS-MACKENZIE",
    "LUZ", "OSCAR FREIRE", "PAULISTA", "PINHEIROS", "REPUBLICA", 
    "SAO PAULO-MORUMBI", "VILA SONIA"
]

# ========== CLASSE PRINCIPAL ==========

class FiltroTransportePublico:
    """
    Sistema inteligente para detecção de terminais, estações e pontos de transporte público
    """
    
    def __init__(self):
        self.estacoes_cptm: Set[str] = set()
        self.estacoes_metro: Set[str] = set()
        self.estacoes_viaquatro: Set[str] = set()
        self.terminais: Set[str] = set()
        self.todos_pontos_transporte: Set[str] = set()
        
        self.regex_prefixos = []
        self.regex_contexto = []
        
        self._carregar_listas_completas()
        self._preparar_padroes_avancados()
    
    def _carregar_listas_completas(self):
        """Carrega todas as listas de estações e terminais"""
        
        # 1. ESTAÇÕES CPTM
        for estacao in CPTM_BASE:
            self.estacoes_cptm.add(f"ESTACAO CPTM {estacao}")
            self.estacoes_cptm.add(f"ESTACAO {estacao}")
        
        # 2. METRÔ
        for estacao in METRO_BASE:
            self.estacoes_metro.add(f"METRO {estacao}")
            self.estacoes_metro.add(f"ESTACAO METRO {estacao}")
            self.estacoes_metro.add(f"METRO {estacao}")
        
        # 3. VIAQUATRO
        for estacao in VIAQUATRO_BASE:
            self.estacoes_viaquatro.add(f"VIAQUATRO {estacao}")
            self.estacoes_viaquatro.add(f"ESTACAO VIAQUATRO {estacao}")
            self.estacoes_viaquatro.add(f"METRO VIAQUATRO {estacao}")
            self.estacoes_viaquatro.add(f"ESTACAO METRO VIAQUATRO {estacao}")
        
        # 4. TERMINAIS
        self.terminais = set(TERMINAIS)
        
        # 5. CONJUNTO COMPLETO
        self.todos_pontos_transporte = (
            self.estacoes_cptm | 
            self.estacoes_metro | 
            self.estacoes_viaquatro | 
            self.terminais
        )
        
        logging.info(f"📊 FiltroTransporte: {len(self.estacoes_cptm)} CPTM, "
                    f"{len(self.estacoes_metro)} metrô, {len(self.estacoes_viaquatro)} ViaQuatro, "
                    f"{len(self.terminais)} terminais")
    
    def _preparar_padroes_avancados(self):
        """Prepara padrões regex para detecção inteligente"""
        
        # Padrões para prefixos
        prefixos = [
            r'^ESTA[CÇ][AÃ]O\s+',
            r'^METR[ÔO]\s+', 
            r'^VIAQUATRO\s+',
            r'^CPTM\s+',
            r'^TERMINAL\s+',
            r'^RODOVIARIO\s+'
        ]
        
        # Padrões para contexto
        padroes_contexto = [
            r'\bESTA[CÇ][AÃ]O\s+[A-Z]+\s*[A-Z\-]*\b',
            r'\bMETR[ÔO]\s+[A-Z]+\s*[A-Z\-]*\b',
            r'\bVIAQUATRO\s+[A-Z]+\s*[A-Z\-]*\b',
            r'\bCPTM\s+[A-Z]+\s*[A-Z\-]*\b',
            r'\bTERMINAL\s+[A-Z]+\s*[A-Z\-]*\b',
            r'\bRODOVIARIO\s+[A-Z]+\s*[A-Z\-]*\b'
        ]
        
        self.regex_prefixos = [re.compile(padrao, re.IGNORECASE) for padrao in prefixos]
        self.regex_contexto = [re.compile(padrao, re.IGNORECASE) for padrao in padroes_contexto]
    
    def _normalizar_para_busca(self, texto: str) -> str:
        """Normaliza texto para busca case-insensitive e sem acentos"""
        if not texto:
            return ""
        
        texto = texto.upper().strip()
        
        # Remover acentos
        substituicoes = {
            'Á': 'A', 'À': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
            'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E', 
            'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
            'Ó': 'O', 'Ò': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
            'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U',
            'Ç': 'C', 'Ñ': 'N',
        }
        
        for char, substituir in substituicoes.items():
            texto = texto.replace(char, substituir)
        
        # Normalizar espaços e caracteres
        texto = re.sub(r'[^A-Z0-9\s\-]', '', texto)
        texto = re.sub(r'\s+', ' ', texto)
        
        return texto.strip()
    
    def _busca_exata_otimizada(self, logradouro: str) -> bool:
        """Busca otimizada nas listas completas"""
        logradouro_norm = self._normalizar_para_busca(logradouro)
        
        if not logradouro_norm:
            return False
        
        for ponto in self.todos_pontos_transporte:
            ponto_norm = self._normalizar_para_busca(ponto)
            
            if (ponto_norm == logradouro_norm or 
                ponto_norm in logradouro_norm or 
                logradouro_norm in ponto_norm):
                logging.debug(f"🎯 FiltroTransporte - Exato: '{logradouro}' → '{ponto}'")
                return True
        
        return False
    
    def _detectar_por_prefixo(self, logradouro: str) -> bool:
        """Detecta prefixos de transporte"""
        for padrao in self.regex_prefixos:
            if padrao.match(logradouro):
                logging.debug(f"🎯 FiltroTransporte - Prefixo: '{logradouro}'")
                return True
        return False
    
    def _detectar_por_contexto(self, logradouro: str) -> bool:
        """Detecta padrões em qualquer posição"""
        for padrao in self.regex_contexto:
            if padrao.search(logradouro):
                match = padrao.search(logradouro)
                logging.debug(f"🎯 FiltroTransporte - Contexto: '{logradouro}' → '{match.group()}'")
                return True
        return False
    
    def _busca_por_nome_estacao(self, logradouro: str) -> bool:
        """Busca por nomes de estações sem prefixo"""
        logradouro_norm = self._normalizar_para_busca(logradouro)
        palavras = re.findall(r'\b[A-Z]{3,}\b', logradouro_norm)
        
        if not palavras:
            return False
        
        for ponto in self.todos_pontos_transporte:
            ponto_norm = self._normalizar_para_busca(ponto)
            palavras_comuns = [p for p in palavras if p in ponto_norm]
            
            if len(palavras_comuns) >= 1 and len(palavras_comuns[0]) >= 4:
                logging.debug(f"🎯 FiltroTransporte - Nome: '{logradouro}' → '{ponto}'")
                return True
        
        return False
    
    def eh_ponto_transporte(self, logradouro: str) -> bool:
        """
        Verifica se é um ponto de transporte público
        
        Args:
            logradouro: Nome do logradouro a ser verificado
            
        Returns:
            bool: True se for terminal, estação ou ponto de transporte
        """
        if not logradouro:
            return False
        
        logradouro_upper = logradouro.upper().strip()
        
        # Estratégias em ordem de prioridade/performance
        if self._detectar_por_prefixo(logradouro_upper):
            return True
        
        if self._busca_exata_otimizada(logradouro_upper):
            return True
        
        if self._detectar_por_contexto(logradouro_upper):
            return True
        
        if self._busca_por_nome_estacao(logradouro_upper):
            return True
        
        return False

# ========== FUNÇÕES DE INTERFACE ==========

# Instância global para uso otimizado
_filtro_transporte_instance = None

def get_filtro_transporte() -> FiltroTransportePublico:
    """Retorna a instância única do filtro (singleton pattern)"""
    global _filtro_transporte_instance
    if _filtro_transporte_instance is None:
        _filtro_transporte_instance = FiltroTransportePublico()
    return _filtro_transporte_instance

def eh_ponto_transporte(logradouro: str) -> bool:
    """
    Função conveniente para verificar se um logradouro é ponto de transporte
    
    Args:
        logradouro: Nome do logradouro a ser verificado
        
    Returns:
        bool: True se for terminal, estação ou ponto de transporte
    """
    filtro = get_filtro_transporte()
    return filtro.eh_ponto_transporte(logradouro)

def filtrar_dataframe_transporte(df: pd.DataFrame, coluna_logradouro: str = 'logradouro_PMSP') -> pd.DataFrame:
    """
    Filtra um DataFrame marcando pontos de transporte como 'NAO ENCONTRADO'
    
    Args:
        df: DataFrame com os resultados
        coluna_logradouro: Nome da coluna que contém o logradouro
        
    Returns:
        pd.DataFrame: DataFrame filtrado
    """
    if df.empty:
        return df
    
    df_result = df.copy()
    filtro = get_filtro_transporte()
    pontos_detectados = 0
    
    for idx, row in df_result.iterrows():
        logradouro = str(row.get(coluna_logradouro, ''))
        
        if filtro.eh_ponto_transporte(logradouro):
            pontos_detectados += 1
            logging.info(f"🚫 FiltroTransporte: '{logradouro}'")
            df_result.at[idx, coluna_logradouro] = "NAO ENCONTRADO"
            df_result.at[idx, "codlog"] = ""
            if "latitude_geocode" in df_result.columns:
                df_result.at[idx, "latitude_geocode"] = None
            if "longitude_geocode" in df_result.columns:
                df_result.at[idx, "longitude_geocode"] = None
    
    if pontos_detectados > 0:
        logging.info(f"🎯 FiltroTransporte: {pontos_detectados} ponto(s) detectado(s)")
    
    return df_result