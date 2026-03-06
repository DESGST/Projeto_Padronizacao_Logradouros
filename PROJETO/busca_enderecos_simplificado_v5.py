from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pathlib import Path
import re
from unidecode import unidecode
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ✅ CACHE SIMPLES EM MEMÓRIA
_MODELO_CACHE = None

def _normalize_text(text: str) -> str:
    """Normaliza texto mantendo apóstrofos"""
    if not isinstance(text, str):
        return ""
    text = unidecode(text).upper().strip()
    text = re.sub(r'[^A-Z0-9\s\']', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def _carregar_modelo_memoria(cadastro_path: str) -> tuple[pd.DataFrame, TfidfVectorizer, np.ndarray]:
    """Carrega modelo uma vez e mantém em memória"""
    global _MODELO_CACHE
    
    if _MODELO_CACHE is not None:
        return _MODELO_CACHE
    
    logging.info("🔨 Carregando modelo em memória...")
    start_time = time.time()
    
    # Carregar dados
    df_cadastro = pd.read_excel(cadastro_path)
    
    # ✅ VERIFICAR COLUNAS
    if 'LOCAL' not in df_cadastro.columns:
        raise KeyError("Coluna 'LOCAL' não encontrada no cadastro")
    if 'codlog' not in df_cadastro.columns:
        raise KeyError("Coluna 'codlog' não encontrada no cadastro")
    
    df_base = df_cadastro[['codlog', 'LOCAL']].copy()
    df_base = df_base.dropna(subset=['LOCAL', 'codlog'])
    df_base['codlog'] = df_base['codlog'].astype(str).str.strip()
    df_base['LOCAL'] = df_base['LOCAL'].astype(str).str.strip()
    
    # TF-IDF
    textos = [_normalize_text(t) for t in df_base['LOCAL'].tolist()]
    
    vectorizer = TfidfVectorizer(
        analyzer='char_wb',
        ngram_range=(2, 4),
        min_df=2,
        max_df=0.9,
        lowercase=False
    )
    
    tfidf_matrix = vectorizer.fit_transform(textos)
    
    _MODELO_CACHE = (df_base, vectorizer, tfidf_matrix)
    
    tempo = time.time() - start_time
    logging.info("✅ Modelo carregado em %.2f segundos", tempo)
    
    return _MODELO_CACHE

def busca_candidatos_df(
    endereco: str,
    cadastro_path: str | Path,
    top_k: int = 20,
    min_sim: int = 80,
) -> pd.DataFrame:
    """Busca RÁPIDA com cache em memória"""
    try:
        start_time = time.time()
        
        # ✅ USAR CACHE
        df_base, vectorizer, tfidf_matrix = _carregar_modelo_memoria(str(cadastro_path))
        
        # Busca
        query_norm = _normalize_text(endereco)
        if not query_norm:
            return pd.DataFrame()
        
        query_vec = vectorizer.transform([query_norm])
        similarities = cosine_similarity(query_vec, tfidf_matrix).flatten()
        
        # Top K
        indices_validos = np.where(similarities >= (min_sim / 100))[0]
        if len(indices_validos) == 0:
            return pd.DataFrame()
            
        top_indices = indices_validos[np.argsort(-similarities[indices_validos])][:top_k]
        
        resultados_df = pd.DataFrame({
            'codlog': df_base.iloc[top_indices]['codlog'].values,
            'LOCAL': df_base.iloc[top_indices]['LOCAL'].values,
            'SIMILARIDADE': [int(round(s * 100)) for s in similarities[top_indices]]
        })
        
        tempo = time.time() - start_time
        if tempo > 0.1:
            logging.info("✅ %d candidatos em %.3f segundos", len(resultados_df), tempo)
        
        return resultados_df
        
    except Exception as e:
        logging.error("❌ Erro: %s", e)
        return pd.DataFrame()