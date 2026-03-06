import pandas as pd
import os
import shutil
from datetime import datetime
from processamento_resiliente import processar_sinistros_resiliente
import logging  # <--- Faltou esse carinha aqui!
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= CONFIGURAÇÕES =================
# O arquivo que você baixa a cada quinzena (contém TUDO ou apenas o mês novo)
ARQUIVO_ENTRADA_NOVO = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\infosiga\planilhas_infosiga\sinistros_infosiga_2026-02-20_14-29.xlsx"

# O seu "Banco de Dados" Mestre (onde ficam os dados já tratados acumulados)
ARQUIVO_HISTORICO_MESTRE = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\dados_cet_pre_tratados.xlsx"

# Pasta temporária para processamento
PASTA_TEMP = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Funções Compartilhadas\DB_DES\data\cet\temp"

# Nome da coluna que identifica unicamente cada linha (Chave Primária)
COLUNA_ID = "id_sinistro" 

def aplicar_vias_complexas(df_novos):
    print("\n🛤️  Iniciando mapeamento de Vias Complexas...")
    
    # Caminho do arquivo do Bruno
    caminho_vias_cplx = r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\Arquivos_de_teste\ARQUIVOS_DE_HOMOLOGACAO\VIAS_CPLX.xlsx"
    
    if not os.path.exists(caminho_vias_cplx):
        print("   ⚠️ Arquivo de Vias Complexas não encontrado! As colunas ficarão vazias.")
        df_novos['logradouros_com_vias_cplx'] = None
        df_novos['via_cplx'] = 'NÃO'
        return df_novos

    # Carrega as Vias Complexas
    df_vias_cplx = pd.read_excel(caminho_vias_cplx)

    # BLINDAGEM: Garante que os códigos sejam strings sem ".0" no final para o mapeamento bater certinho
    df_vias_cplx['codlogb'] = df_vias_cplx['codlogb'].astype(str).str.replace(r'\.0$', '', regex=True)
    df_novos['codlog_string'] = df_novos['codlog'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Cria o dicionário do Bruno
    mapeamento_vias_cplx = df_vias_cplx.set_index('codlogb')['via_cplx'].to_dict()

    df_resultado = df_novos.copy()

    # Aplica o mapeamento
    df_resultado['logradouros_com_vias_cplx'] = df_resultado['codlog_string'].map(mapeamento_vias_cplx)

    # Preenche os que não são via complexa com o logradouro_PMSP (ou original se PMSP falhar)
    coluna_fallback = 'logradouro_PMSP' if 'logradouro_PMSP' in df_resultado.columns else 'logradouro'
    df_resultado['logradouros_com_vias_cplx'] = df_resultado['logradouros_com_vias_cplx'].combine_first(df_resultado[coluna_fallback])

    # Adiciona SIM/NÃO
    df_resultado['via_cplx'] = df_resultado['codlog_string'].map(mapeamento_vias_cplx).notna().map({True: 'SIM', False: 'NÃO'})

    # Limpa a coluna temporária
    df_resultado = df_resultado.drop(columns=['codlog_string'])
    
    print(f"   ✅ Mapeamento concluído! Encontradas {len(df_resultado[df_resultado['via_cplx'] == 'SIM'])} vias complexas no lote novo.")
    
    return df_resultado

def realizar_enriquecimento_parcial():
    print("🚀 INICIANDO ENRIQUECIMENTO PARCIAL (INCREMENTAL)")
    
    # 1. Verificar e Criar pastas necessárias
    if not os.path.exists(PASTA_TEMP):
        os.makedirs(PASTA_TEMP)
        
    # 2. Carregar o Histórico (para saber o que já fizemos)
    ids_processados = set()
    df_historico = pd.DataFrame()
    
    if os.path.exists(ARQUIVO_HISTORICO_MESTRE):
        print(f"📚 Lendo histórico existente: {ARQUIVO_HISTORICO_MESTRE}")
        # Ler apenas a coluna de ID para ser rápido na verificação
        df_ids = pd.read_excel(ARQUIVO_HISTORICO_MESTRE, usecols=[COLUNA_ID])
        ids_processados = set(df_ids[COLUNA_ID].astype(str).unique())
        print(f"   ✅ {len(ids_processados)} registros já existem no histórico.")
    else:
        print("⚠️  Nenhum histórico encontrado. Será criado um novo arquivo Mestre.")

    # 3. Carregar o Arquivo Novo (Baixado recentemente)
    print(f"📥 Lendo arquivo novo: {ARQUIVO_ENTRADA_NOVO}")
    df_novo_completo = pd.read_excel(ARQUIVO_ENTRADA_NOVO)
    
    # Garantir que a coluna ID seja string para comparação
    df_novo_completo[COLUNA_ID] = df_novo_completo[COLUNA_ID].astype(str)
    
    # 4. Filtrar APENAS o que é novo (Delta)
    # Pega linhas onde o ID NÃO está no set de ids_processados
    df_delta = df_novo_completo[~df_novo_completo[COLUNA_ID].isin(ids_processados)]
    
    qtd_novos = len(df_delta)
    
    if qtd_novos == 0:
        print("💤 Nenhum registro novo encontrado. Todos os IDs já estão no histórico.")
        return

    print(f"✨ Encontrados {qtd_novos} NOVOS registros para processar.")
    
    # 5. Salvar o Delta em um arquivo temporário para o processador ler
    nome_arquivo_temp = "novos_registros_para_processar.xlsx"
    caminho_temp = os.path.join(PASTA_TEMP, nome_arquivo_temp)
    df_delta.to_excel(caminho_temp, index=False)
    
    # 6. EXECUTAR O PROCESSAMENTO RESILIENTE (Reutilizando seu código)
    print("\n⚙️  Chamando módulo de processamento resiliente...")
    
    # Nota: O sufixo será adicionado pelo processador, ex: "_enriquecido"
    arquivos_saida = processar_sinistros_resiliente(
        arquivos=nome_arquivo_temp,
        pasta_entrada=PASTA_TEMP,
        pasta_saida=PASTA_TEMP,
        sufixo="_delta_processado",
        batch_size=1000,     # Lotes menores para ser mais rápido
        max_tentativas=3,
        ciclos_maximos=2
    )
    
    if not arquivos_saida:
        print("❌ O processamento não retornou arquivos de saída.")
        return

    arquivo_processado = arquivos_saida[0] # Pega o primeiro (e único) arquivo gerado
    
# 7. Unificar (Append) no Histórico Mestre com Trava de Segurança
    print("\n🔗 Verificando necessidade de unificação (Append)...")
    
    df_delta_tratado = pd.read_excel(arquivo_processado)
    
    # ==========================================================
    # APLICAMOS SÓ NO DELTA NOVO
    # ==========================================================
    df_delta_tratado = aplicar_vias_complexas(df_delta_tratado)
    # ==========================================================
    
    df_delta_tratado[COLUNA_ID] = df_delta_tratado[COLUNA_ID].astype(str) 
    
    if os.path.exists(ARQUIVO_HISTORICO_MESTRE):
        df_historico = pd.read_excel(ARQUIVO_HISTORICO_MESTRE)
        df_historico[COLUNA_ID] = df_historico[COLUNA_ID].astype(str)
        
        # TRAVA DE SEGURANÇA: Cruza os IDs para garantir que não faremos append duplicado
        ids_no_mestre = set(df_historico[COLUNA_ID].unique())
        
        # Mantém no delta APENAS as linhas cujo ID não está no Mestre
        df_para_append = df_delta_tratado[~df_delta_tratado[COLUNA_ID].isin(ids_no_mestre)]
        qtd_novos_para_append = len(df_para_append)
        
        if qtd_novos_para_append == 0:
            print("✅ O Arquivo Mestre já está atualizado com esses registros. Nenhum append extra será realizado.")
            return # Encerra o script com segurança
            
        print(f"✨ Inserindo {qtd_novos_para_append} registros inéditos no Histórico Mestre...")
        
        # Fazer backup antes de sobrescrever
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = ARQUIVO_HISTORICO_MESTRE.replace(".xlsx", f"_BACKUP_{timestamp}.xlsx")
        shutil.copy2(ARQUIVO_HISTORICO_MESTRE, backup_path)
        print(f"   💾 Backup criado: {os.path.basename(backup_path)}")
        
        # Faz o append apenas do que realmente é novo
        df_final = pd.concat([df_historico, df_para_append], ignore_index=True)
    else:
        print("⚠️ Nenhum histórico anterior encontrado. Criando um novo Mestre do zero.")
        df_final = df_delta_tratado
        
    # 8. Salvar Histórico Atualizado (ESTRATÉGIA SEGURA PARA REDE)
    print(f"💾 Salvando Histórico Atualizado com {len(df_final)} registros totais...")
    
    # 8.1 - Salva na pasta temp local primeiro (evita falhas silenciosas do Pandas na rede)
    nome_temp_salvamento = "temp_mestre_atualizado.xlsx"
    caminho_temp_salvamento = os.path.join(PASTA_TEMP, nome_temp_salvamento)
    
    print("   ⏳ Escrevendo arquivo temporário local...")
    df_final.to_excel(caminho_temp_salvamento, index=False)
    
    # 8.2 - Copia o arquivo pronto para o destino final na rede
    print("   🚚 Transferindo arquivo final para a rede de forma segura...")
    shutil.copy2(caminho_temp_salvamento, ARQUIVO_HISTORICO_MESTRE)
    
    # 8.3 - Limpa o arquivo temporário
    os.remove(caminho_temp_salvamento)
    
    print("\n✅ PROCESSO DE ENRIQUECIMENTO PARCIAL CONCLUÍDO COM SUCESSO!")
    print(f"   - Registros adicionados hoje: {len(df_para_append) if 'df_para_append' in locals() else len(df_final)}")
    print(f"   - Arquivo Mestre: {ARQUIVO_HISTORICO_MESTRE}")

if __name__ == "__main__":
    realizar_enriquecimento_parcial()
