import pandas as pd
import os
import shutil

# ================= CONFIGURAÇÕES =================
ARQUIVO_HISTORICO_MESTRE = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\dados_cet_pre_tratados.xlsx"
CAMINHO_VIAS_CPLX = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\Arquivos_de_teste\ARQUIVOS_DE_HOMOLOGACAO\VIAS_CPLX.xlsx"
PASTA_TEMP = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\temp"

print("⏳ Lendo o arquivo Mestre completo...")
df_mestre = pd.read_excel(ARQUIVO_HISTORICO_MESTRE)

# Garante que as colunas existem no Mestre (para não dar erro)
if 'logradouros_com_vias_cplx' not in df_mestre.columns:
    df_mestre['logradouros_com_vias_cplx'] = None
if 'via_cplx' not in df_mestre.columns:
    df_mestre['via_cplx'] = None

# 🎯 IDENTIFICA APENAS AS LINHAS QUE FALTAM (as 2.244 novas)
# Procura onde a 'via_cplx' está vazia (NaN)
mask_faltantes = df_mestre['via_cplx'].isna()
qtd_faltantes = mask_faltantes.sum()

if qtd_faltantes == 0:
    print("✅ Tudo certo! Todas as 204 mil linhas já possuem a classificação de vias complexas.")
else:
    print(f"✨ Encontradas {qtd_faltantes} linhas sem vias complexas. Aplicando a regra apenas nelas...")
    
    # 1. Carrega as Vias Complexas
    df_vias_cplx = pd.read_excel(CAMINHO_VIAS_CPLX)
    df_vias_cplx['codlogb'] = df_vias_cplx['codlogb'].astype(str).str.replace(r'\.0$', '', regex=True)
    mapeamento_vias_cplx = df_vias_cplx.set_index('codlogb')['via_cplx'].to_dict()

    # 2. Isola as linhas que precisam ser atualizadas
    df_atualizar = df_mestre.loc[mask_faltantes].copy()
    df_atualizar['codlog_string'] = df_atualizar['codlog'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # 3. Aplica a regra
    df_atualizar['logradouros_com_vias_cplx'] = df_atualizar['codlog_string'].map(mapeamento_vias_cplx)
    coluna_fallback = 'logradouro_PMSP' if 'logradouro_PMSP' in df_atualizar.columns else 'logradouro'
    df_atualizar['logradouros_com_vias_cplx'] = df_atualizar['logradouros_com_vias_cplx'].combine_first(df_atualizar[coluna_fallback])
    
    df_atualizar['via_cplx'] = df_atualizar['codlog_string'].map(mapeamento_vias_cplx).notna().map({True: 'SIM', False: 'NÃO'})
    
    # 4. Devolve os dados calculados para o DataFrame Mestre
    df_mestre.loc[mask_faltantes, 'logradouros_com_vias_cplx'] = df_atualizar['logradouros_com_vias_cplx']
    df_mestre.loc[mask_faltantes, 'via_cplx'] = df_atualizar['via_cplx']
    
    print("💾 Salvando o Arquivo Mestre corrigido de forma segura na rede...")
    if not os.path.exists(PASTA_TEMP):
        os.makedirs(PASTA_TEMP)
        
    caminho_temp = os.path.join(PASTA_TEMP, "mestre_corrigido_temp.xlsx")
    df_mestre.to_excel(caminho_temp, index=False)
    shutil.copy2(caminho_temp, ARQUIVO_HISTORICO_MESTRE)
    os.remove(caminho_temp)
    
    print("\n✅ SUCESSO! Apenas as linhas vazias foram atualizadas. O arquivo está pronto para o banco de dados")