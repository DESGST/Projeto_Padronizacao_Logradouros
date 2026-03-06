# circunscricao.py
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def definir_circunscricao_via_shapefile(df: pd.DataFrame, col_lat: str, col_lon: str, shapefile_path: str) -> pd.DataFrame:
    """
    Define se as coordenadas estão dentro da circunscrição do município de SP
    
    Args:
        df: DataFrame com coordenadas
        col_lat: Nome da coluna de latitude
        col_lon: Nome da coluna de longitude  
        shapefile_path: Caminho para o shapefile do município
    
    Returns:
        DataFrame com coluna 'fora_circunscricao' (True/False)
    """
    try:
        # Carregar shapefile do município
        gdf_municipio = gpd.read_file(shapefile_path)
        
        # Criar geometrias dos pontos
        geometry = [Point(xy) for xy in zip(df[col_lon], df[col_lat])]
        gdf_pontos = gpd.GeoDataFrame(df, geometry=geometry, crs=gdf_municipio.crs)
        
        # Verificar quais pontos estão dentro do município
        dentro_municipio = gpd.sjoin(gdf_pontos, gdf_municipio, how="left", predicate="within")
        
        # Criar coluna fora_circunscricao (True = fora, False = dentro)
        df_result = df.copy()
        df_result['fora_circunscricao'] = dentro_municipio['index_right'].isna()
        
        logging.info(f"✅ Circunscrição aplicada: {df_result['fora_circunscricao'].sum()} pontos fora do município")
        return df_result
        
    except Exception as e:
        logging.error(f"❌ Erro na circunscrição: {e}")
        # Em caso de erro, marcar todos como dentro da circunscrição
        df_result = df.copy()
        df_result['fora_circunscricao'] = False
        return df_result