import pandas as pd
import numpy as np
import os

# ==============================================================================
# 🛠️ CONFIGURAÇÕES DE HIERARQUIA (Sua Lista Personalizada)
# ==============================================================================
REGRAS_HIERARQUIA = {
    'CN': ['CN1', 'CN2', 'CN3'],          
    'LE': ['LE1', 'LE2', 'LE3', 'LE4'],    
    'SE': ['SE1', 'SE2', 'SE3'],           
    'SU': ['SU1', 'SU2', 'SU3', 'SU4'],    
    'SO': ['SO1', 'SO2', 'SO3'],           
    'OE': ['OE1', 'OE2', 'OE3', 'OE4'],    
    'NO': ['NO1', 'NO2', 'NO3'],           
    'MB': ['PB', 'TT', 'TS']               # Onde a regra antiga falhava
}

def comparar_e_validar_completo(path_antigo, path_novo):
    print(f"📂 Carregando arquivos...")
    print(f"   Antigo: {path_antigo}")
    print(f"   Novo:   {path_novo}")
    
    # Função de carregamento seguro
    def carregar(p):
        try:
            if p.endswith('.csv'): 
                return pd.read_csv(p, sep=';', encoding='utf-8', dtype=str)
            return pd.read_excel(p, dtype=str)
        except Exception as e:
            print(f"❌ Erro crítico ao ler: {p}\n   Erro: {e}")
            return pd.DataFrame()

    df_old = carregar(path_antigo)
    df_new = carregar(path_novo)

    if df_old.empty or df_new.empty:
        return

    # Ajusta tamanho para o menor (caso tenha diferenças de linhas cortadas)
    m = min(len(df_old), len(df_new))
    df_old = df_old.iloc[:m]
    df_new = df_new.iloc[:m]
    
    # Cria coluna com número da linha do Excel (Index + 2)
    df_new['Linha_Excel'] = df_new.index + 2

    # Garante que as colunas existem
    colunas_foco = ['GET', 'DET', 'SUB', 'Distrito_Nome', 'Regiao_Nome', 'Classificacao']
    for col in colunas_foco:
        if col not in df_old.columns: df_old[col] = np.nan
        if col not in df_new.columns: df_new[col] = np.nan

    # ==========================================================================
    # 1. RELATÓRIO DE PREENCHIMENTO (Quem preencheu mais?)
    # ==========================================================================
    print("\n" + "="*80)
    print("📊 1. COMPARAÇÃO DE PREENCHIMENTO (Antigo vs Novo)")
    print("="*80)
    print(f"{'COLUNA':<15} | {'ANTIGO':<10} | {'NOVO':<10} | {'DIFERENÇA':<10}")
    print("-" * 80)
    
    melhorou_geral = True
    
    for col in colunas_foco:
        n_old = df_old[col].notna().sum()
        n_new = df_new[col].notna().sum()
        diff = n_new - n_old
        sinal = "+" if diff > 0 else ""
        
        print(f"{col:<15} | {n_old:<10} | {n_new:<10} | {sinal}{diff}")
        
        if diff < 0:
            melhorou_geral = False

    if melhorou_geral:
        print("\n✅ CONCLUSÃO: O arquivo NOVO tem mais (ou a mesma quantidade de) dados.")
    else:
        print("\n⚠️ ALERTA: O arquivo novo tem MENOS dados em algumas colunas.")

    # ==========================================================================
    # 2. DIVERGÊNCIAS (Onde mudou o valor?)
    # ==========================================================================
    print("\n" + "="*80)
    print("🔍 2. ONDE OS DADOS MUDARAM? (Divergências)")
    print("="*80)
    
    for col in colunas_foco:
        # Normaliza strings
        s_old = df_old[col].fillna('').astype(str).str.strip().str.upper()
        s_new = df_new[col].fillna('').astype(str).str.strip().str.upper()
        
        mask_diff = (s_old != s_new)
        qtd = mask_diff.sum()
        
        if qtd > 0:
            print(f"⚠️ {col}: {qtd} linhas mudaram de valor.")
        else:
            print(f"✅ {col}: Nenhuma mudança de valor (dados consistentes).")

    # ==========================================================================
    # 3. VALIDAÇÃO DE HIERARQUIA (GET vs DET)
    # ==========================================================================
    print("\n" + "="*80)
    print("🕵️‍♂️ 3. AUDITORIA DE HIERARQUIA (Baseada na lista: CN, LE, MB...)")
    print("="*80)
    
    def checar_hierarquia(row):
        # Limpeza
        def limpar(txt):
            return str(txt).replace('GET', '').replace('DET', '').replace('SQLPRODUCAO:', '').strip().upper()

        sigla_get = limpar(row.get('GET', ''))
        sigla_det = limpar(row.get('DET', ''))
        
        # Ignora vazios
        if sigla_get in ['', 'NAN', 'NONE'] or sigla_det in ['', 'NAN', 'NONE']:
            return "IGNORAR"
            
        # Validação Exata pelo Dicionário
        if sigla_get in REGRAS_HIERARQUIA:
            if sigla_det in REGRAS_HIERARQUIA[sigla_get]:
                return "OK"
            else:
                return f"ERRO ({sigla_get} x {sigla_det})"
        
        return f"ALERTA (GET '{sigla_get}' desconhecida)"

    # Aplica
    df_new['AUDITORIA'] = df_new.apply(checar_hierarquia, axis=1)
    
    # Filtra erros
    erros = df_new[~df_new['AUDITORIA'].isin(['OK', 'IGNORAR'])]
    
    print(f"Total analisado: {len(df_new)}")
    print(f"Hierarquia OK:   {len(df_new[df_new['AUDITORIA'] == 'OK'])}")
    print(f"Inconsistências: {len(erros)}")
    
    if not erros.empty:
        print("\n❌ ERROS ENCONTRADOS (Amostra):")
        cols_erro = ['Linha_Excel', 'GET', 'DET', 'AUDITORIA', 'latitude_geocode', 'longitude_geocode']
        
        # Imprime formatado para terminal
        print("-" * 100)
        print(erros[cols_erro].head(15).to_string(index=False))
        print("-" * 100)
        
        print("\n📊 Resumo dos erros:")
        print(erros.groupby(['GET', 'DET']).size().reset_index(name='Qtd').sort_values('Qtd', ascending=False).to_string(index=False))
    else:
        print("\n✅ SUCESSO! A hierarquia (GET/DET) está 100% correta no arquivo novo.")

# ==========================================================================
# 👇 EXECUÇÃO
# ==========================================================================
if __name__ == "__main__":
    arq_antigo = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\dados_cet_pre_tratados_20251216.xlsx"
    arq_novo   = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\dados_cet_pre_tratados_20251216_ENRIQUECIDO_FULL.xlsx"
    
    comparar_e_validar_completo(arq_antigo, arq_novo)