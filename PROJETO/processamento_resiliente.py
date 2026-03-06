import pandas as pd
import time
import os
from datetime import datetime
from unidecode import unidecode
import warnings
from typing import List, Union
from IPython.display import display, HTML
from pathlib import Path
from typing import List, Union, Optional, Dict

warnings.filterwarnings("ignore")

class ProcessadorSinistrosResiliente:
    """
    CLASSE RESILIENTE - Combina a robustez do ProcessadorSinistros com resiliência
    """

    def __init__(self, batch_size=100, max_tentativas_por_ciclo=3, ciclos_maximos=4):
        self.config = {
            "cadastro_path": r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\cadastroRuas_V5_13_10_2025.xlsx",
            "shapefile_sp": r"\\cet-020059\DES_Dados\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\LIMITE_MUNICIPAL\LIMITE_MUNICIPAL_4674\LIMITE_MUNICIPAL_4674.shp",
            "coluna_endereco": "logradouro",
            "coluna_numero": "numero",
            "min_sim": 70,
            "top_k": 20
        }
        
        # ✅ NOVOS PARÂMETROS DE RESILIÊNCIA
        self.batch_size = batch_size
        self.max_tentativas_por_ciclo = max_tentativas_por_ciclo  # 3 tentativas por ciclo
        self.ciclos_maximos = ciclos_maximos  # 4 ciclos = 12 tentativas totais
        self.tempo_espera_ciclo = 600  # 10 minutos entre ciclos
        
        # ✅ SISTEMA DE CHECKPOINTS
        self.checkpoint_dir = "checkpoints_resilientes"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        print(f"⚙️  PROCESSADOR RESILIENTE INICIADO:")
        print(f"   - Batch size: {self.batch_size}")
        print(f"   - Tentativas por ciclo: {self.max_tentativas_por_ciclo}")
        print(f"   - Ciclos máximos: {self.ciclos_maximos}")
        print(f"   - Espera entre ciclos: {self.tempo_espera_ciclo//60}min")
        print(f"   - Checkpoints: {self.checkpoint_dir}")
        
        # ✅ INICIAR LOGS
        self._salvar_log_processamento("PROCESSAMENTO INICIADO")
        
        self._importar_modulos()

    def _salvar_resultados_finais(self, df_final: pd.DataFrame, pasta_saida: Path,
                            nome_base: str, sufixo: str, salvar_excel: bool) -> dict:
        """Salva resultados SEM colunas de controle - VERSÃO CORRIGIDA"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivos_gerados = {}
        
        try:
            # ✅ CORREÇÃO: CRIAR CÓPIA SEM COLUNAS DE CONTROLE
            df_para_salvar = df_final.copy()
            
            # ✅ LISTA DEFINITIVA DE COLUNAS PARA REMOVER
            colunas_para_remover = [
                'linha_origem', 
                'status_processamento', 
                'motivo_status', 
                'timestamp_processamento'
            ]
            
            # ✅ REMOVER EFETIVAMENTE
            for coluna in colunas_para_remover:
                if coluna in df_para_salvar.columns:
                    df_para_salvar = df_para_salvar.drop(columns=[coluna])
            
            print(f"🚫 Colunas removidas: {colunas_para_remover}")
            print(f"📊 Colunas restantes: {list(df_para_salvar.columns)}")
            
            # ✅ SALVAR ARQUIVOS (já sem colunas de controle)
            caminho_parquet = pasta_saida / f"{nome_base}{sufixo}.parquet"
            df_para_salvar.to_parquet(caminho_parquet, index=False)
            arquivos_gerados['parquet'] = str(caminho_parquet)
            
            if salvar_excel:
                caminho_excel = pasta_saida / f"{nome_base}{sufixo}.xlsx"
                df_para_salvar.to_excel(caminho_excel, index=False)
                arquivos_gerados['excel'] = str(caminho_excel)
            
            return arquivos_gerados
            
        except Exception as e:
            print(f"❌ Erro ao salvar: {e}")
            raise

    # ✅ CORRIGIR O MÉTODO _salvar_aba_logs_execucao PARA RECEBER O PARÂMETRO
    def _salvar_aba_logs_execucao(self, excel_writer, df_final_completo=None):
        """Cria aba de logs no arquivo Excel usando dados completos"""
        try:
            # ✅ DADOS DO RELATÓRIO (usar df_final_completo se disponível)
            relatorio_data = []
            
            # CABEÇALHO
            relatorio_data.append(["RELATÓRIO DE EXECUÇÃO - PROCESSAMENTO RESILIENTE"])
            relatorio_data.append(["Data/Hora", datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
            relatorio_data.append(["Arquivo processado", self.relatorio.get("arquivo_origem", "N/A")])
            relatorio_data.append([])
            
            # ESTATÍSTICAS GERAIS
            relatorio_data.append(["ESTATÍSTICAS GERAIS"])
            relatorio_data.append(["Total de registros", self.relatorio["total_registros"]])
            relatorio_data.append(["Registros processados", self.relatorio["registros_processados"]])
            relatorio_data.append(["Lotes processados", self.relatorio["lotes_processados"]])
            relatorio_data.append(["Sucessos", self.relatorio["sucessos"]])
            relatorio_data.append(["Falhas", self.relatorio["falhas"]])
            taxa_sucesso = (self.relatorio['sucessos']/self.relatorio['total_registros']*100) if self.relatorio['total_registros'] > 0 else 0
            relatorio_data.append(["Taxa de sucesso", f"{taxa_sucesso:.1f}%"])
            relatorio_data.append([])
            
            # ✅ ESTATÍSTICAS DETALHADAS DO PROCESSAMENTO (se temos os dados completos)
            if df_final_completo is not None and 'status_processamento' in df_final_completo.columns:
                relatorio_data.append(["ESTATÍSTICAS DETALHADAS DO PROCESSAMENTO"])
                status_counts = df_final_completo['status_processamento'].value_counts()
                for status, count in status_counts.items():
                    percentual = (count / len(df_final_completo)) * 100
                    relatorio_data.append([f"Registros {status}", f"{count} ({percentual:.1f}%)"])
                relatorio_data.append([])
            
            # TEMPOS
            horas = int(self.relatorio['tempo_total'] // 3600)
            minutos = int((self.relatorio['tempo_total'] % 3600) // 60)
            segundos = self.relatorio['tempo_total'] % 60
            relatorio_data.append(["TEMPO DE PROCESSAMENTO"])
            relatorio_data.append(["Tempo total", f"{horas:02d}:{minutos:02d}:{segundos:05.2f}"])
            if self.relatorio['lotes_processados'] > 0:
                tempo_medio = self.relatorio['tempo_total'] / self.relatorio['lotes_processados']
                relatorio_data.append(["Tempo médio por lote", f"{tempo_medio:.1f}s"])
                velocidade = self.relatorio['registros_processados'] / self.relatorio['tempo_total']
                relatorio_data.append(["Velocidade média", f"{velocidade:.1f} regs/segundo"])
            relatorio_data.append([])
            
            # CONFIGURAÇÕES
            relatorio_data.append(["CONFIGURAÇÕES UTILIZADAS"])
            for config, valor in self.relatorio.get("configuracoes_utilizadas", {}).items():
                relatorio_data.append([config, valor])
            relatorio_data.append([])
            
            # ERROS (se houver)
            if self.relatorio['erros_detalhados']:
                relatorio_data.append(["PRINCIPAIS ERROS ENCONTRADOS"])
                for i, erro in enumerate(self.relatorio['erros_detalhados'][:10]):
                    relatorio_data.append([f"Erro {i+1}", f"Lote {erro['lote_id']}: {erro['erro'][:100]}"])
                relatorio_data.append([])
            
            # ESTATÍSTICAS POR LOTE
            relatorio_data.append(["ESTATÍSTICAS POR LOTE"])
            relatorio_data.append(["Lote", "Registros", "Sucessos", "Falhas", "Tempo(s)"])
            for estat in self.relatorio['estatisticas_por_lote']:
                relatorio_data.append([
                    estat['lote_id'],
                    estat['registros_processados'],
                    estat['sucessos_lote'],
                    estat['falhas_lote'],
                    f"{estat['tempo_processamento']:.1f}"
                ])
            
            # CONVERTER PARA DATAFRAME E SALVAR
            df_logs = pd.DataFrame(relatorio_data)
            df_logs.to_excel(excel_writer, sheet_name='LOG_EXECUCAO', index=False, header=False)
            
        except Exception as e:
            self.logger.error(f"Erro ao criar aba de logs: {e}")

        # ✅ CORRIGIR O MÉTODO _salvar_log_texto_completo
        def _salvar_log_texto_completo(self, pasta_saida: Path, timestamp: str, df_final_completo=None):
            """Salva log completo em arquivo de texto"""
            try:
                log_texto_path = pasta_saida / f"log_execucao_detalhado_{timestamp}.txt"
                
                with open(log_texto_path, 'w', encoding='utf-8') as f:
                    f.write("=" * 80 + "\n")
                    f.write("RELATÓRIO DETALHADO DE EXECUÇÃO - PROCESSAMENTO RESILIENTE\n")
                    f.write("=" * 80 + "\n\n")
                    
                    f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                    f.write(f"Arquivo de origem: {self.relatorio.get('arquivo_origem', 'N/A')}\n\n")
                    
                    f.write("ESTATÍSTICAS GERAIS:\n")
                    f.write(f"  • Total de registros: {self.relatorio['total_registros']:,}\n")
                    f.write(f"  • Registros processados: {self.relatorio['registros_processados']:,}\n")
                    f.write(f"  • Lotes processados: {self.relatorio['lotes_processados']}\n")
                    f.write(f"  • Sucessos: {self.relatorio['sucessos']:,}\n")
                    f.write(f"  • Falhas: {self.relatorio['falhas']:,}\n")
                    taxa_sucesso = (self.relatorio['sucessos']/self.relatorio['total_registros']*100) if self.relatorio['total_registros'] > 0 else 0
                    f.write(f"  • Taxa de sucesso: {taxa_sucesso:.1f}%\n\n")
                    
                    # ✅ ADICIONAR ESTATÍSTICAS DETALHADAS SE DISPONÍVEL
                    if df_final_completo is not None and 'status_processamento' in df_final_completo.columns:
                        f.write("ESTATÍSTICAS DETALHADAS:\n")
                        status_counts = df_final_completo['status_processamento'].value_counts()
                        for status, count in status_counts.items():
                            percentual = (count / len(df_final_completo)) * 100
                            f.write(f"  • {status}: {count} ({percentual:.1f}%)\n")
                            f.write("\n")
                    
                    # TEMPOS
                    horas = int(self.relatorio['tempo_total'] // 3600)
                    minutos = int((self.relatorio['tempo_total'] % 3600) // 60)
                    segundos = self.relatorio['tempo_total'] % 60
                    f.write("TEMPO DE PROCESSAMENTO:\n")
                    f.write(f"  • Total: {horas:02d}:{minutos:02d}:{segundos:05.2f}\n")
                    if self.relatorio['lotes_processados'] > 0:
                        tempo_medio = self.relatorio['tempo_total'] / self.relatorio['lotes_processados']
                        f.write(f"  • Médio por lote: {tempo_medio:.1f}s\n")
                        velocidade = self.relatorio['registros_processados'] / self.relatorio['tempo_total']
                        f.write(f"  • Velocidade: {velocidade:.1f} regs/segundo\n\n")
                    
                    # CONFIGURAÇÕES
                    f.write("CONFIGURAÇÕES UTILIZADAS:\n")
                    for config, valor in self.relatorio.get("configuracoes_utilizadas", {}).items():
                        f.write(f"  • {config}: {valor}\n")
                    f.write("\n")
                    
                    # ERROS DETALHADOS
                    if self.relatorio['erros_detalhados']:
                        f.write("ERROS DETALHADOS:\n")
                        for i, erro in enumerate(self.relatorio['erros_detalhados']):
                            f.write(f"  {i+1}. Lote {erro['lote_id']} - {erro['timestamp']}\n")
                            f.write(f"      {erro['erro']}\n")
                        f.write("\n")
                    
                    # ESTATÍSTICAS POR LOTE
                    f.write("ESTATÍSTICAS POR LOTE:\n")
                    for estat in self.relatorio['estatisticas_por_lote']:
                        f.write(f"  • Lote {estat['lote_id']}: {estat['registros_processados']} regs, "
                               f"{estat['sucessos_lote']}✅ {estat['falhas_lote']}❌, "
                               f"{estat['tempo_processamento']:.1f}s\n")
                    
                    f.write("\n" + "=" * 80 + "\n")
                    f.write("FIM DO RELATÓRIO\n")
                    f.write("=" * 80 + "\n")
                
                self.logger.info(f"Log detalhado salvo: {log_texto_path}")
                self.relatorio["arquivos_gerados"]["log_texto"] = str(log_texto_path)
                
            except Exception as e:
                self.logger.error(f"Erro ao salvar log em texto: {e}")
        
    def _importar_modulos(self):
        """Importa os módulos necessários"""
        try:
            from buscaEndereco_CET_v12_0 import buscar_endereco_candidatos, enriquecer_candidatos_geoserver
            from circunscricao import definir_circunscricao_via_shapefile
            
            self.buscar_endereco_candidatos = buscar_endereco_candidatos
            self.enriquecer_candidatos_geoserver = enriquecer_candidatos_geoserver
            self.definir_circunscricao = definir_circunscricao_via_shapefile
            print("✅ Módulos importados com sucesso!")
        except ImportError as e:
            print(f"❌ Erro na importação: {e}")
            raise

    def processar_resiliente(self, arquivos, pasta_entrada=None, pasta_saida=None, 
                           sufixo="_resiliente", usar_timestamp=False):
        """
        Processa arquivos com sistema resiliente de lotes
        """
        inicio_total = time.time()
        
        if isinstance(arquivos, str):
            arquivos = [arquivos]
        
        resultados = []
        
        for arquivo in arquivos:
            display(HTML(f"<h3>🚀 PROCESSAMENTO RESILIENTE: {arquivo}</h3>"))
            
            try:
                # Construir caminhos
                caminho_entrada = os.path.join(pasta_entrada, arquivo) if pasta_entrada else arquivo
                
                if not os.path.exists(caminho_entrada):
                    display(HTML(f"<div style='color: red'>❌ Arquivo não encontrado: {caminho_entrada}</div>"))
                    continue
                
                # Processar com resiliência
                caminho_saida = self._processar_arquivo_resiliente(
                    caminho_entrada, pasta_saida, sufixo, usar_timestamp
                )
                
                if caminho_saida:
                    resultados.append(caminho_saida)
                    display(HTML(f"<div style='color: green'>✅ CONCLUÍDO: {os.path.basename(caminho_saida)}</div>"))
                    
            except Exception as e:
                display(HTML(f"<div style='color: red'>❌ ERRO em {arquivo}: {e}</div>"))
                import traceback
                traceback.print_exc()
        
        # Relatório final
        tempo_total = time.time() - inicio_total
        self._gerar_relatorio_final(resultados, tempo_total)
        
        return resultados

    def _processar_arquivo_resiliente(self, caminho_entrada, pasta_saida, sufixo, usar_timestamp):
        """Processa um arquivo com sistema de lotes resiliente"""
        try:
            # ✅ 1. CARREGAR DADOS
            df_input = pd.read_excel(caminho_entrada)
            df_input = df_input.drop_duplicates().dropna(how="all")
            
            print(f"📊 DADOS CARREGADOS: {len(df_input)} registros")
            
            # ✅ 2. PRÉ-PROCESSAMENTO
            df_input = self._preprocessar_dados(df_input)
            
            # ✅ 3. DIVIDIR EM LOTES
            lotes = self._dividir_em_lotes(df_input)
            print(f"📦 LOTES CRIADOS: {len(lotes)} lotes de {self.batch_size} registros")
            
            # ✅ 4. PROCESSAR LOTES COM RESILIÊNCIA
            resultados_lotes = []
            total_sucessos = 0
            total_falhas = 0
            
            for i, lote in enumerate(lotes):
                lote_id = i + 1
                print(f"\n🔄 PROCESSANDO LOTE {lote_id}/{len(lotes)}...")
                
                resultado_lote = self._processar_lote_resiliente(lote, lote_id)
                
                if resultado_lote is not None and len(resultado_lote) > 0:
                    resultados_lotes.append(resultado_lote)
                    sucessos_lote = len(resultado_lote[resultado_lote['status_processamento'] == 'SUCESSO'])
                    falhas_lote = len(resultado_lote[resultado_lote['status_processamento'] != 'SUCESSO'])
                    
                    total_sucessos += sucessos_lote
                    total_falhas += falhas_lote
                    
                    print(f"   ✅ Lote {lote_id}: {sucessos_lote}✅ {falhas_lote}❌")
                
                # ✅ SALVAR CHECKPOINT A CADA 5 LOTES
                if lote_id % 5 == 0:
                    self._salvar_checkpoint(resultados_lotes, lote_id)
            
            # ✅ 5. CONSOLIDAR RESULTADOS
            if resultados_lotes:
                df_final = pd.concat(resultados_lotes, ignore_index=True)
                df_final = self._posprocessar_resultados(df_final)
                
                # ✅ 6. SALVAR RESULTADO FINAL
                return self._salvar_resultado_resiliente(
                    df_final, caminho_entrada, pasta_saida, sufixo, usar_timestamp,
                    total_sucessos, total_falhas
                )
            else:
                print("⚠️  Nenhum resultado obtido do processamento")
                return None
                
        except Exception as e:
            print(f"❌ ERRO no processamento resiliente: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _preprocessar_dados(self, df_input):
        """Pré-processa os dados antes do processamento em lotes"""
        print("🔧 PRÉ-PROCESSANDO DADOS...")
        
        # ✅ NORMALIZAR TEXTO
        for c in df_input.select_dtypes(include=["object"]).columns:
            df_input[c] = df_input[c].astype(str).apply(unidecode).str.strip()
        
        # ✅ PRESERVAR COORDENADAS ORIGINAIS
        df_input = self._preservar_coordenadas_originais(df_input)
        
        # ✅ PROCESSAR COORDENADAS
        df_input = self._processar_coordenadas_com_validacao(df_input)
        
        # ✅ APLICAR CIRCUNSCRIÇÃO
        df_input = self._aplicar_circunscricao(df_input)
        
        return df_input

    def _dividir_em_lotes(self, df_input):
        """Divide o DataFrame em lotes menores"""
        return [df_input[i:i + self.batch_size] for i in range(0, len(df_input), self.batch_size)]
    
    def _salvar_log_tentativa(self, lote_id, tentativa_numero, status, erro=None):
        """Salva log detalhado de cada tentativa em arquivo TXT"""
        try:
            log_file = Path("logs_processamento.txt")
            
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                linha_log = f"{timestamp} | Lote {lote_id} | Tentativa {tentativa_numero} | {status}"
                
                if erro:
                    linha_log += f" | Erro: {erro}"
                
                f.write(linha_log + "\n")
                
        except Exception as e:
            print(f"⚠️  Erro ao salvar log: {e}")

    def _salvar_log_processamento(self, mensagem):
        """Salva log geral do processamento"""
        try:
            log_file = Path("logs_geral_processamento.txt")
            
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} | {mensagem}\n")
                
        except Exception as e:
            print(f"⚠️  Erro ao salvar log geral: {e}")
    
    def _processar_lote_resiliente(self, lote, lote_id, pasta_saida=None, nome_base=None):
        """Processa um lote com sistema de tentativas inteligente"""
        tentativas_totais = 0
        max_tentativas_por_ciclo = self.max_tentativas_por_ciclo
        ciclos_maximos = self.ciclos_maximos
        tempo_espera_ciclo = self.tempo_espera_ciclo
        
        for ciclo in range(ciclos_maximos):
            tentativas_ciclo = 0
            
            while tentativas_ciclo < max_tentativas_por_ciclo:
                tentativas_totais += 1
                tentativas_ciclo += 1
                
                try:
                    print(f"   🎯 Ciclo {ciclo+1}, Tentativa {tentativas_ciclo}...")
                    resultado = self._processar_lote(lote, lote_id)
                    
                    # ✅ SALVAR LOG DE SUCESSO
                    self._salvar_log_tentativa(lote_id, tentativas_totais, "SUCESSO", None, pasta_saida, nome_base)
                    return resultado
                    
                except Exception as e:
                    erro_msg = str(e)[:200]
                    print(f"   ❌ Tentativa {tentativas_totais} falhou: {erro_msg}")
                    
                    # ✅ SALVAR LOG DE FALHA
                    self._salvar_log_tentativa(lote_id, tentativas_totais, "FALHA", erro_msg, pasta_saida, nome_base)
                    
                    if tentativas_ciclo < max_tentativas_por_ciclo:
                        wait_curto = min(30, tentativas_ciclo * 10)
                        print(f"   ⏳ Aguardando {wait_curto}s...")
                        time.sleep(wait_curto)
            
            if ciclo < ciclos_maximos - 1:
                tempo_restante = (ciclos_maximos - ciclo - 1) * tempo_espera_ciclo
                print(f"   🔄 Ciclo {ciclo+1} completo. Próximo ciclo em {tempo_espera_ciclo//60}min")
                time.sleep(tempo_espera_ciclo)
        
        print(f"   💥 Todas as {tentativas_totais} tentativas falharam para o lote {lote_id}")
        self._salvar_log_tentativa(lote_id, tentativas_totais, "ESGOTADO", "Todas as tentativas falharam", pasta_saida, nome_base)
        return self._criar_fallback_lote(lote, lote_id, f"ESGOTADO: {tentativas_totais} tentativas")
    
    def _salvar_log_tentativa(self, lote_id, tentativa_numero, status, erro=None, pasta_saida=None, nome_base=None):
        """Salva log detalhado de cada tentativa em arquivo TXT junto com a saída"""
        try:
            if pasta_saida and nome_base:
                log_file = Path(pasta_saida) / f"{nome_base}_log_execucao.txt"
            else:
                log_file = Path("logs_processamento.txt")
            
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                linha_log = f"{timestamp} | Lote {lote_id} | Tentativa {tentativa_numero} | {status}"
                
                if erro:
                    linha_log += f" | Erro: {erro}"
                
                f.write(linha_log + "\n")
                
        except Exception as e:
            print(f"⚠️  Erro ao salvar log: {e}")

    def _salvar_log_processamento(self, mensagem, pasta_saida=None, nome_base=None):
        """Salva log geral do processamento junto com a saída"""
        try:
            if pasta_saida and nome_base:
                log_file = Path(pasta_saida) / f"{nome_base}_log_execucao.txt"
            else:
                log_file = Path("logs_geral_processamento.txt")
            
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} | {mensagem}\n")
                
        except Exception as e:
            print(f"⚠️  Erro ao salvar log geral: {e}")

    def _processar_lote(self, lote, lote_id):
        """Processa um lote individual"""
        resultados = []
        
        for idx, row in lote.iterrows():
            try:
                resultado = self._processar_registro(row, idx + (lote_id - 1) * self.batch_size)
                if resultado is not None:
                    resultados.append(resultado)
            except Exception as e:
                # Fallback individual para registro com erro
                resultado_fallback = self._criar_resultado_fallback(
                    row, idx + (lote_id - 1) * self.batch_size, f"ERRO_REGISTRO: {str(e)[:50]}"
                )
                resultados.append(resultado_fallback)
        
        if resultados:
            return pd.concat(resultados, ignore_index=True)
        else:
            return self._criar_fallback_lote(lote, lote_id, "LOTE_VAZIO")

    
    def _processar_registro(self, row, index):
        """Processa um registro individual - VERSÃO FINAL PRODUÇÃO"""
        try:
            logradouro = str(row.get(self.config["coluna_endereco"], "") or row.get("endereco", ""))
            
            # ✅ VALIDAÇÃO DE ENDEREÇO
            if not self._validar_endereco_resiliente(logradouro):
                return self._criar_resultado_fallback(row, index, "ENDEREÇO_INVALIDO")
            
            # ✅ BUSCAR O NÚMERO CORRETAMENTE
            numero = ""
            possiveis_colunas_numero = [
                "numero_logradouro",           # coluna real nos seus dados
                self.config["coluna_numero"],  # "numero" da configuração  
                "numero",                      # fallback
                "num",                         # fallback
            ]
            
            for col_num in possiveis_colunas_numero:
                if col_num in row and pd.notna(row[col_num]) and str(row[col_num]).strip() not in ['', '0']:
                    numero = str(row[col_num])
                    break
            
            numero_processado = self._processar_numero_resiliente(numero)

            # ✅ VALIDAR COORDENADAS
            if 'latitude_original' not in row or 'longitude_original' not in row:
                return self._criar_resultado_fallback(row, index, "COORDENADAS_AUSENTES")
            
            lat_original = row['latitude_original']
            lon_original = row['longitude_original']
            
            if pd.isna(lat_original) or pd.isna(lon_original):
                return self._criar_resultado_fallback(row, index, "COORDENADAS_INVALIDAS")
            
            # ✅ BUSCA DE CANDIDATOS (COM NÚMERO)
            df_candidatos = self.buscar_endereco_candidatos(
                endereco=logradouro,
                numero=numero_processado,
                lat_origem=lat_original,
                lon_origem=lon_original,
                max_candidatos=35
            )
            
            # ✅ PROCESSAR RESULTADOS
            if (isinstance(df_candidatos, pd.DataFrame) and not df_candidatos.empty and 
                df_candidatos.iloc[0]["logradouro_PMSP"] != "NAO ENCONTRADO"):
                
                # Ordenar por qualidade
                if "distancia_km" in df_candidatos.columns:
                    df_ordenado = df_candidatos.sort_values(by="distancia_km", ascending=True)
                elif "similaridade" in df_candidatos.columns:
                    df_ordenado = df_candidatos.sort_values(by="similaridade", ascending=False)
                else:
                    df_ordenado = df_candidatos
                
                melhor_candidato = df_ordenado.iloc[0]
                
                # Enriquecer
                df_melhor_candidato = pd.DataFrame([melhor_candidato])
                df_enriquecido = self.enriquecer_candidatos_geoserver(df_melhor_candidato)
                
                if not df_enriquecido.empty:
                    df_result = df_enriquecido.copy()
                else:
                    df_result = df_melhor_candidato.copy()
                
                # ✅ METADADOS
                df_result["linha_origem"] = index + 1
                df_result["status_processamento"] = "SUCESSO"
                df_result["motivo_status"] = None
                df_result["timestamp_processamento"] = datetime.now().isoformat()
                
                # ✅ PRESERVAR COLUNAS ORIGINAIS
                for col in row.index:
                    if col not in df_result.columns:
                        df_result[col] = row[col]
                
                return df_result
            else:
                return self._criar_resultado_fallback(row, index, "SEM_CANDIDATOS")
                
        except Exception as e:
            return self._criar_resultado_fallback(row, index, f"ERRO_PROCESSAMENTO: {str(e)[:50]}")
    

    def _criar_resultado_fallback(self, row, index, motivo):
        """Cria resultado de fallback padronizado"""
        dados_base = {
            "linha_origem": index + 1,
            "status_processamento": "FALLBACK",
            "motivo_status": motivo,
            "timestamp_processamento": datetime.now().isoformat(),
            "latitude_geocode": None,
            "longitude_geocode": None,
            "codlog": None,
            "logradouro_PMSP": None,
            "similaridade": None,
            "distancia_km": None,
            "fora_circunscricao": row.get("fora_circunscricao", False) if "fora_circunscricao" in row else False
        }
        
        # Adicionar colunas originais
        for col in row.index:
            if col not in dados_base:
                dados_base[col] = row[col]
        
        return pd.DataFrame([dados_base])

    def _criar_fallback_lote(self, lote, lote_id, motivo):
        """Cria fallback para lote inteiro em caso de falha"""
        resultados = []
        for idx, row in lote.iterrows():
            resultado = self._criar_resultado_fallback(
                row, idx + (lote_id - 1) * self.batch_size, f"FALHA_LOTE: {motivo}"
            )
            resultados.append(resultado)
        
        return pd.concat(resultados, ignore_index=True) if resultados else None

    def _salvar_checkpoint(self, resultados_lotes, lote_id):
        """Salva checkpoint do processamento"""
        try:
            if resultados_lotes:
                df_checkpoint = pd.concat(resultados_lotes, ignore_index=True)
                checkpoint_path = os.path.join(self.checkpoint_dir, f"checkpoint_lote_{lote_id}.parquet")
                df_checkpoint.to_parquet(checkpoint_path, index=False)
                print(f"   💾 Checkpoint salvo: Lote {lote_id}")
        except Exception as e:
            print(f"   ⚠️  Erro ao salvar checkpoint: {e}")

    def _posprocessar_resultados(self, df_final):
        """Pós-processa os resultados finais"""
        # Remover duplicatas mantendo o melhor resultado por linha
        df_final = df_final.drop_duplicates(subset=['linha_origem'], keep='first')
        
        # Reordenar colunas
        return self._reordenar_colunas(df_final)

    def _salvar_resultado_resiliente(self, df_final, caminho_entrada, pasta_saida, sufixo, usar_timestamp, sucessos, falhas):
        """Salva resultado SEM colunas de controle"""
        nome_base = os.path.splitext(os.path.basename(caminho_entrada))[0]
        
        if usar_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_saida = f"{nome_base}{sufixo}_{timestamp}.xlsx"
        else:
            nome_saida = f"{nome_base}{sufixo}.xlsx"
        
        if pasta_saida:
            caminho_saida = os.path.join(pasta_saida, nome_saida)
        else:
            caminho_saida = os.path.join(os.path.dirname(caminho_entrada), nome_saida)
        
        os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
        
        # ✅ CORREÇÃO: REMOVER COLUNAS DE CONTROLE ANTES DE SALVAR
        df_para_salvar = df_final.copy()
        
        colunas_para_remover = [
            'linha_origem', 
            'status_processamento', 
            'motivo_status', 
            'timestamp_processamento'
        ]
        
        for coluna in colunas_para_remover:
            if coluna in df_para_salvar.columns:
                df_para_salvar = df_para_salvar.drop(columns=[coluna])
        
        print(f"🚫 Colunas removidas: {[col for col in colunas_para_remover if col in df_final.columns]}")
        print(f"📊 Colunas restantes: {list(df_para_salvar.columns)}")
        
        # ✅ SALVAR ARQUIVO LIMPO
        df_para_salvar.to_excel(caminho_saida, index=False)
        
        # Relatório resiliente
        self._gerar_relatorio_resiliente(df_final, caminho_entrada, caminho_saida, sucessos, falhas)
        
        return caminho_saida

    def _gerar_relatorio_resiliente(self, df_final, caminho_entrada, caminho_saida, sucessos, falhas):
        """Gera relatório detalhado do processamento resiliente"""
        total = len(df_final)
        
        print(f"\n{'='*70}")
        print("📊 RELATÓRIO RESILIENTE - PROCESSAMENTO EM LOTES")
        print(f"{'='*70}")
        print(f"📁 Arquivo processado: {os.path.basename(caminho_entrada)}")
        print(f"📈 Total de registros: {total:,}")
        print(f"🎯 Taxa de sucesso: {sucessos:,} ({sucessos/total*100:.1f}%)")
        print(f"⚠️  Falhas/fallbacks: {falhas:,} ({falhas/total*100:.1f}%)")
        print(f"📦 Tamanho do lote: {self.batch_size}")
        print(f"🔄 Tentativas por ciclo: {self.max_tentativas_por_ciclo}")  # ✅ CORRIGIDO
        print(f"🔄 Ciclos máximos: {self.ciclos_maximos}")  # ✅ ADICIONAR
        
        # Estatísticas detalhadas
        if 'status_processamento' in df_final.columns:
            print(f"\n📋 DISTRIBUIÇÃO DE STATUS:")
            status_counts = df_final['status_processamento'].value_counts()
            for status, count in status_counts.items():
                print(f"   • {status}: {count:,} ({count/total*100:.1f}%)")
        
        if 'fora_circunscricao' in df_final.columns:
            fora_circ = df_final['fora_circunscricao'].sum()
            print(f"\n📍 FORA DA CIRCUNSCRIÇÃO: {fora_circ:,} ({fora_circ/total*100:.1f}%)")
        
        print(f"\n💾 ARQUIVO SALVO: {caminho_saida}")
        print(f"{'='*70}")

    def _gerar_relatorio_final(self, resultados, tempo_total):
        """Relatório final consolidado"""
        horas = int(tempo_total // 3600)
        minutos = int((tempo_total % 3600) // 60)
        segundos = tempo_total % 60

        display(HTML(f"""
        <div style='background: #f0f8ff; padding: 15px; border-radius: 10px; border-left: 5px solid #007acc;'>
            <h3>🎉 PROCESSAMENTO RESILIENTE CONCLUÍDO!</h3>
            <p><strong>⏱️ Tempo total:</strong> {horas:02d}:{minutos:02d}:{segundos:05.2f}</p>
            <p><strong>📁 Arquivos processados:</strong> {len(resultados)}</p>
            <p><strong>📊 Configuração:</strong> Lotes de {self.batch_size}, {self.max_tentativas_por_ciclo} tentativas por ciclo</p>  <!-- ✅ CORRIGIDO -->
        </div>
        """))

    # ✅ MÉTODOS AUXILIARES (mantidos do seu código original)
    def _preservar_coordenadas_originais(self, df_input):
        """Preserva coordenadas originais"""
        cols_lower = {c.lower(): c for c in df_input.columns}
        
        col_lat_original = next((cols_lower[c] for c in cols_lower if "lat" in c), None)
        col_lon_original = next((cols_lower[c] for c in cols_lower if "lon" in c or "lng" in c), None)
        
        if col_lat_original and col_lat_original in df_input.columns:
            df_input = df_input.rename(columns={
                col_lat_original: 'latitude_original',
                col_lon_original: 'longitude_original'
            })
            print("✅ Coordenadas originais preservadas")
        else:
            print("⚠️  Colunas de coordenadas não encontradas")
            df_input['latitude_original'] = None
            df_input['longitude_original'] = None
        
        return df_input

    def _validar_endereco_resiliente(self, logradouro):
        """Validação robusta de endereço"""
        if not logradouro or pd.isna(logradouro):
            return False
        
        logradouro_clean = str(logradouro).strip()
        if len(logradouro_clean) < 6:
            return False
        
        palavras_invalidas = ['desconhecido', 'unknown', 'não informado', 'nao informado', 
                             'n/a', 'null', 'none', 'nan', 'indefinido', 'teste']
        
        return logradouro_clean.lower() not in palavras_invalidas

    def _processar_numero_resiliente(self, numero):
        """Processa número de forma resiliente"""
        if numero is None or pd.isna(numero):
            return ""
        
        if isinstance(numero, (int, float)):
            return str(int(numero)) if numero > 0 else ""
        
        numero_str = str(numero).strip()
        if numero_str in ['', '0', '0.0', 'NaN', 'nan', 'None', 'null']:
            return ""
        
        if numero_str.endswith('.0'):
            numero_str = numero_str[:-2]
        
        return numero_str

    def _processar_coordenadas_com_validacao(self, df_input):
        """Processa coordenadas com validação"""
        if 'latitude_original' not in df_input.columns or 'longitude_original' not in df_input.columns:
            return df_input
        
        df_input['latitude_original'] = pd.to_numeric(
            df_input['latitude_original'].astype(str).str.replace(",", "."), 
            errors='coerce'
        )
        df_input['longitude_original'] = pd.to_numeric(
            df_input['longitude_original'].astype(str).str.replace(",", "."), 
            errors='coerce'
        )
        
        return df_input

    def _aplicar_circunscricao(self, df_input):
        """Aplica circunscrição"""
        try:
            if 'latitude_original' in df_input.columns and 'longitude_original' in df_input.columns:
                df_input = self.definir_circunscricao(
                    df_input, 'latitude_original', 'longitude_original', self.config["shapefile_sp"]
                )
                print("✅ Circunscrição aplicada")
            else:
                df_input["fora_circunscricao"] = False
        except Exception as e:
            print(f"⚠️  Erro na circunscrição: {e}")
            df_input["fora_circunscricao"] = False
        
        return df_input

    def _reordenar_colunas(self, df_final):
        """Reordena colunas"""
        ordem_preferencial = [
            'id_sinistro', 'linha_origem', 'status_processamento', 'motivo_status', 'timestamp_processamento',
            'logradouro', 'numero_logradouro', 'endereco',
            'latitude_original', 'longitude_original',
            'latitude_geocode', 'longitude_geocode',
            'codlog', 'logradouro_PMSP', 'DET', 'GET', 'SUB',
            'Distrito_Nome', 'Regiao_Nome', 'Classificacao',
            'similaridade', 'distancia_km', 'fora_circunscricao'
        ]
        
        colunas_existentes = [c for c in ordem_preferencial if c in df_final.columns]
        colunas_restantes = [c for c in df_final.columns if c not in colunas_existentes]
        
        return df_final[colunas_existentes + colunas_restantes]

# ✅ FUNÇÃO DE CONVENIÊNCIA
def processar_sinistros_resiliente(arquivos, pasta_entrada=None, pasta_saida=None, 
                                 sufixo="_resiliente", usar_timestamp=False,
                                 batch_size=100, max_tentativas=3, ciclos_maximos=4):
    """
    Função rápida para processamento resiliente em Jupyter
    """
    processor = ProcessadorSinistrosResiliente(
        batch_size=batch_size, 
        max_tentativas_por_ciclo=max_tentativas,
        ciclos_maximos=ciclos_maximos
    )
    return processor.processar_resiliente(arquivos, pasta_entrada, pasta_saida, sufixo, usar_timestamp)