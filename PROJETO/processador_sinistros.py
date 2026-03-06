import pandas as pd
import time
import os
from datetime import datetime
from unidecode import unidecode
import warnings
from typing import List, Union
from IPython.display import display, HTML

warnings.filterwarnings("ignore")

class ProcessadorSinistros:
    """
    Classe otimizada para uso em Jupyter Notebook
    """
    
    def __init__(self):
        self.config = {
            "cadastro_path": r"X:\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\cadastroRuas_V5_13_10_2025.xlsx",
            "shapefile_sp": r"X:\Dados\ARQUIVO DE CÓDIGOS\Modulos_de_tratamento_de_dados\LIMITE_MUNICIPAL\LIMITE_MUNICIPAL_4674\LIMITE_MUNICIPAL_4674.shp",
            "coluna_endereco": "logradouro",
            "coluna_numero": "numero",
            "min_sim": 70,
            "top_k": 20
        }
        
        # ✅ DEBUG DA CONFIGURAÇÃO
        print(f"⚙️  CONFIGURAÇÃO CARREGADA:")
        print(f"   - Coluna endereço: '{self.config['coluna_endereco']}'")
        print(f"   - Coluna número: '{self.config['coluna_numero']}'")
        
        # Barra de progresso do Jupyter
        try:
            from tqdm.auto import tqdm
            self.tqdm = tqdm
        except ImportError:
            self.tqdm = None
            
        self._importar_modulos()
    
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
    
    def processar(self, arquivos, pasta_entrada=None, pasta_saida=None, sufixo="_enriquecido", usar_timestamp=False):
        """
        Processa arquivos de forma flexível
        """
        # 🔄 ADICIONAR CONTROLE DE TEMPO
        inicio_total = time.time()
        
        # Normalizar entrada
        if isinstance(arquivos, str):
            arquivos = [arquivos]
        
        resultados = []
        
        for arquivo in arquivos:
            display(HTML(f"<h3>🚀 Processando: {arquivo}</h3>"))
            
            try:
                # Construir caminhos
                caminho_entrada = os.path.join(pasta_entrada, arquivo) if pasta_entrada else arquivo
                
                if not os.path.exists(caminho_entrada):
                    display(HTML(f"<div style='color: red'>❌ Arquivo não encontrado: {caminho_entrada}</div>"))
                    continue
                
                # Processar
                caminho_saida = self._processar_arquivo(
                    caminho_entrada, pasta_saida, sufixo, usar_timestamp
                )
                
                if caminho_saida:
                    resultados.append(caminho_saida)
                    display(HTML(f"<div style='color: green'>✅ Concluído: {os.path.basename(caminho_saida)}</div>"))
                    
            except Exception as e:
                display(HTML(f"<div style='color: red'>❌ Erro em {arquivo}: {e}</div>"))
                import traceback
                traceback.print_exc()
        
        # 🔄 CALCULAR TEMPO TOTAL
        tempo_total = time.time() - inicio_total
        horas = int(tempo_total // 3600)
        minutos = int((tempo_total % 3600) // 60)
        segundos = tempo_total % 60
        
        # Resumo final com tempo
        display(HTML(f"<h3>📊 Resumo: {len(resultados)} arquivo(s) processado(s)</h3>"))
        
        print(f"\n⏱️  TEMPO TOTAL DE EXECUÇÃO: {horas:02d}:{minutos:02d}:{segundos:05.2f}")
        if resultados:
            tempo_por_arquivo = tempo_total / len(resultados)
            print(f"📈 VELOCIDADE MÉDIA: {tempo_por_arquivo:.2f} segundos por arquivo")
        
        return resultados
    
    def _processar_arquivo(self, caminho_entrada, pasta_saida, sufixo, usar_timestamp):
        """Processa um arquivo individual"""
        
        try:
            # Ler dados
            df_input = pd.read_excel(caminho_entrada)
            df_input = df_input.drop_duplicates().dropna(how="all")
            
            # ✅ DEBUG DAS COLUNAS DISPONÍVEIS
            print(f"📊 COLUNAS DISPONÍVEIS NO DATAFRAME:")
            for col in df_input.columns:
                print(f"   - '{col}'")
            
            # ✅ VERIFICAR E APLICAR AUTOMATICAMENTE A COLUNA NÚMERO
            coluna_numero_config = self.config["coluna_numero"]
            if coluna_numero_config not in df_input.columns:
                print(f"⚠️  COLUNA DE NÚMERO NÃO ENCONTRADA: '{coluna_numero_config}'")
                print(f"   📋 Colunas disponíveis: {list(df_input.columns)}")
                
                # Tentar encontrar automaticamente
                colunas_possiveis = [c for c in df_input.columns if 'numero' in c.lower() or 'num' in c.lower()]
                
                if colunas_possiveis:
                    coluna_correta = colunas_possiveis[0]
                    print(f"   🔍 COLUNA ALTERNATIVA ENCONTRADA: '{coluna_correta}'")
                    print(f"   ✅ CONFIGURAÇÃO ATUALIZADA: coluna_numero = '{coluna_correta}'")
                    self.config["coluna_numero"] = coluna_correta
                else:
                    print(f"   🟡 NENHUMA COLUNA ALTERNATIVA ENCONTRADA")
            else:
                print(f"   ✅ COLUNA DE NÚMERO ENCONTRADA: '{coluna_numero_config}'")

            print(f"   🎯 COLUNA DE NÚMERO DEFINITIVA: '{self.config['coluna_numero']}'")
            
            display(HTML(f"<div>📖 Registros carregados: {len(df_input)}</div>"))
            
            # ✅ CORREÇÃO: PRESERVAR COORDENADAS ORIGINAIS (FUNÇÃO QUE ESTAVA FALTANDO)
            df_input = self._preservar_coordenadas_originais(df_input)
            
            # Normalizar texto
            for c in df_input.select_dtypes(include=["object"]).columns:
                df_input[c] = df_input[c].astype(str).apply(unidecode).str.strip()
            
            # Processar coordenadas com validações
            df_input = self._processar_coordenadas_com_validacao(df_input)
            
            # Aplicar circunscrição
            df_input = self._aplicar_circunscricao(df_input)
            
            # Processar endereços
            resultados = []
            total_linhas = len(df_input)
            
            print(f"🔍 Processando {total_linhas} registros...")
            
            for i, row in df_input.iterrows():
                resultado = self._processar_linha_com_validacao(row, i)
                if resultado is not None:
                    resultados.append(resultado)
                
                # Progresso a cada 10 registros
                if (i + 1) % 10 == 0:
                    print(f"   📊 Progresso: {i + 1}/{total_linhas} ({((i + 1)/total_linhas)*100:.1f}%)")
            
            # Consolidar resultados
            if resultados:
                df_final = pd.concat(resultados, ignore_index=True)
                df_final = df_final.drop_duplicates(subset=['linha_origem'], keep='first')
                
                # Reordenar colunas
                df_final = self._reordenar_colunas(df_final)
                
                # Salvar
                return self._salvar_resultado(df_final, caminho_entrada, pasta_saida, sufixo, usar_timestamp)
            else:
                print("⚠️  Nenhum resultado obtido do processamento")
                return None
                
        except Exception as e:
            print(f"❌ Erro no processamento do arquivo: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _preservar_coordenadas_originais(self, df_input):
        """
        ✅ FUNÇÃO QUE ESTAVA FALTANDO!
        Preserva as coordenadas originais antes do processamento
        """
        cols_lower = {c.lower(): c for c in df_input.columns}
        
        col_lat_original = next((cols_lower[c] for c in cols_lower if "lat" in c), None)
        col_lon_original = next((cols_lower[c] for c in cols_lower if "lon" in c or "lng" in c), None)
        
        if col_lat_original and col_lat_original in df_input.columns:
            df_input = df_input.rename(columns={
                col_lat_original: 'latitude_original',
                col_lon_original: 'longitude_original'
            })
            print(f"✅ Coordenadas originais preservadas")
        else:
            print(f"⚠️  Colunas de coordenadas não encontradas")
            # Criar colunas vazias para evitar erros
            df_input['latitude_original'] = None
            df_input['longitude_original'] = None
        
        return df_input

    def _validar_coordenadas(self, lat, lon):
        """Valida se as coordenadas são válidas para SP"""
        try:
            lat_float = float(lat)
            lon_float = float(lon)
            
            # Range geográfico de São Paulo
            if (-24.0 <= lat_float <= -23.3) and (-47.0 <= lon_float <= -46.3):
                return True
            else:
                print(f"      ⚠️  Coordenadas fora do range de SP: ({lat_float}, {lon_float})")
                return False
                
        except (ValueError, TypeError):
            print(f"      ⚠️  Coordenadas inválidas: ({lat}, {lon})")
            return False

    def _validar_endereco(self, logradouro):
        """Valida se o endereço é válido"""
        if not logradouro or pd.isna(logradouro):
            print("      ⚠️  Endereço vazio")
            return False
        
        if len(str(logradouro).strip()) < 6:
            print(f"      ⚠️  Endereço muito curto: '{logradouro}'")
            return False
        
        palavras_invalidas = ['desconhecido', 'unknown', 'não informado', 'nao informado', 
                             'n/a', 'null', 'none', 'nan', 'indefinido', 'teste']
        
        if str(logradouro).lower().strip() in palavras_invalidas:
            print(f"      ⚠️  Endereço inválido: '{logradouro}'")
            return False
        
        return True

    def _processar_coordenadas_com_validacao(self, df_input):
        """Processa coordenadas com validação"""
        print("📍 Processando e validando coordenadas...")
        
        if 'latitude_original' not in df_input.columns or 'longitude_original' not in df_input.columns:
            print("❌ COLUNAS DE COORDENADAS NÃO ENCONTRADAS")
            print("   ✅ Todas as linhas serão mantidas com status 'NÃO DISPONÍVEL'")
            return df_input
        
        # Converter coordenadas para numérico
        df_input['latitude_original'] = pd.to_numeric(
            df_input['latitude_original'].astype(str).str.replace(",", "."), 
            errors='coerce'
        )
        df_input['longitude_original'] = pd.to_numeric(
            df_input['longitude_original'].astype(str).str.replace(",", "."), 
            errors='coerce'
        )
        
        # Contar problemas
        coordenadas_invalidas = df_input[
            df_input['latitude_original'].isna() | 
            df_input['longitude_original'].isna()
        ].shape[0]
        
        if coordenadas_invalidas > 0:
            print(f"⚠️  Encontradas {coordenadas_invalidas} linhas com coordenadas inválidas (NaN)")
        
        # Validar range geográfico
        coordenadas_fora_range = 0
        for idx, row in df_input.iterrows():
            if pd.notna(row['latitude_original']) and pd.notna(row['longitude_original']):
                if not self._validar_coordenadas(row['latitude_original'], row['longitude_original']):
                    coordenadas_fora_range += 1
        
        if coordenadas_fora_range > 0:
            print(f"⚠️  {coordenadas_fora_range} coordenadas fora do range esperado de SP")
        
        print(f"✅ Todas as {len(df_input)} linhas serão mantidas para processamento")
        return df_input
        
    def _processar_numero(self, numero):
        """Processa o número do endereço de forma robusta"""
        if numero is None or pd.isna(numero):
            return ""
        
        if isinstance(numero, (int, float)):
            if numero > 0:
                return str(int(numero))
            else:
                return ""
        
        numero_str = str(numero).strip()
        
        if numero_str in ['', '0', '0.0', 'NaN', 'nan', 'None', 'null']:
            return ""
        
        if numero_str.endswith('.0'):
            numero_str = numero_str[:-2]
        
        return numero_str

    def _criar_resultado_nao_disponivel(self, row, index, motivo):
        """Cria resultado com status 'NÃO DISPONÍVEL'"""
        try:
            dados_base = {
                "linha_origem": index + 1,
                "status_processamento": "NÃO DISPONÍVEL",
                "motivo_status": motivo,
                "latitude_geocode": None,
                "longitude_geocode": None,
                "similaridade": None,
                "distancia_km": None
            }
            
            # Adicionar todas as colunas originais
            for col in row.index:
                if col not in dados_base:
                    dados_base[col] = row[col]
            
            return pd.DataFrame([dados_base])
            
        except Exception as e:
            print(f"      ❌ Erro ao criar resultado NÃO DISPONÍVEL: {e}")
            return None

    def _processar_linha_com_validacao(self, row, index):
        """Processa uma linha individual com validações - VERSÃO OTIMIZADA v12.0"""
        try:
            logradouro = str(row.get(self.config["coluna_endereco"], "") or row.get("endereco", ""))
            
            # VALIDAÇÃO 1: Endereço válido
            if not self._validar_endereco(logradouro):
                return self._criar_resultado_nao_disponivel(row, index, "ENDEREÇO INVÁLIDO")
            
            # Processar número
            numero = row.get(self.config["coluna_numero"], "")
            numero_processado = self._processar_numero(numero)

            # Debug
            if numero_processado:
                print(f"   🔍 [{index + 1}] Número processado: '{numero_processado}' (original: '{numero}')")
            else:
                print(f"   🔍 [{index + 1}] Sem número válido (original: '{numero}')")
            
            # Verificar coordenadas
            if 'latitude_original' not in row or 'longitude_original' not in row:
                return self._criar_resultado_nao_disponivel(row, index, "COORDENADAS AUSENTES")
            
            lat_original = row['latitude_original']
            lon_original = row['longitude_original']
            
            if pd.isna(lat_original) or pd.isna(lon_original):
                return self._criar_resultado_nao_disponivel(row, index, "COORDENADAS INVÁLIDAS (NaN)")
            
            # Avisar se fora do range, mas processar normalmente
            if not self._validar_coordenadas(lat_original, lon_original):
                print(f"      ⚠️  Coordenadas fora do range, mas processando: ({lat_original}, {lon_original})")
            
            # ✅✅✅ VERSÃO 12.0: Busca TODOS os candidatos relevantes
            df_candidatos = self.buscar_endereco_candidatos(
                endereco=logradouro,
                numero=numero_processado,
                lat_origem=lat_original,
                lon_origem=lon_original,
                max_candidatos=35  # ✅ Mantém alto para garantir candidatos relevantes
            )
            
            # Processar resultados
            if isinstance(df_candidatos, pd.DataFrame) and not df_candidatos.empty and df_candidatos.iloc[0]["logradouro_PMSP"] != "NAO ENCONTRADO":
                
                # ✅✅✅ VERSÃO 12.0: Ordenar por qualidade ANTES de selecionar
                if "distancia_km" in df_candidatos.columns and lat_original and lon_original:
                    # Ordenar por distância (mais relevante quando temos coordenadas)
                    df_ordenado = df_candidatos.sort_values(by="distancia_km", ascending=True)
                elif "similaridade" in df_candidatos.columns:
                    # Ordenar por similaridade (quando não temos coordenadas ou distância)
                    df_ordenado = df_candidatos.sort_values(by="similaridade", ascending=False)
                else:
                    # Manter ordem original
                    df_ordenado = df_candidatos
                
                # ✅✅✅ VERSÃO 12.0: Selecionar APENAS o MELHOR candidato para enriquecer
                melhor_candidato = df_ordenado.iloc[0]
                
                print(f"      📊 Candidatos encontrados: {len(df_candidatos)}")
                print(f"      🎯 Melhor candidato selecionado:")
                print(f"         - Logradouro: {melhor_candidato.get('logradouro_PMSP', 'N/A')}")
                if 'distancia_km' in melhor_candidato and pd.notna(melhor_candidato['distancia_km']):
                    print(f"         - Distância: {melhor_candidato['distancia_km']:.3f} km")
                if 'similaridade' in melhor_candidato and pd.notna(melhor_candidato['similaridade']):
                    print(f"         - Similaridade: {melhor_candidato['similaridade']}%")
                
                # ✅✅✅ VERSÃO 12.0: Enriquecer APENAS o candidato selecionado
                df_melhor_candidato = pd.DataFrame([melhor_candidato])
                df_enriquecido = self.enriquecer_candidatos_geoserver(df_melhor_candidato)
                
                if not df_enriquecido.empty:
                    df_result = df_enriquecido.copy()
                else:
                    df_result = df_melhor_candidato.copy()
                
                # Adicionar metadados
                df_result["linha_origem"] = index + 1
                df_result["status_processamento"] = "SUCESSO"
                
                # Adicionar colunas originais
                for col in row.index:
                    if col not in df_result.columns:
                        df_result[col] = row[col]
                
                return df_result
            else:
                print(f"      ⚠️  Nenhum candidato encontrado para geocoding")
                return self._criar_resultado_nao_disponivel(row, index, "GEOCODING SEM RESULTADOS")
            
        except Exception as e:
            print(f"      ❌ Erro na linha {index + 1}: {str(e)[:100]}...")
            return self._criar_resultado_nao_disponivel(row, index, f"ERRO: {str(e)[:50]}")

    def _aplicar_circunscricao(self, df_input):
        """Aplica circunscrição via shapefile"""
        print("🌐 Aplicando circunscrição via shapefile...")
        try:
            if 'latitude_original' in df_input.columns and 'longitude_original' in df_input.columns:
                df_input = self.definir_circunscricao(
                    df_input, 'latitude_original', 'longitude_original', self.config["shapefile_sp"]
                )
                print("   ✅ Circunscrição aplicada com sucesso")
            else:
                print("   ⚠️  Coordenadas não disponíveis para circunscrição")
                df_input["fora_circunscricao"] = False
            
        except Exception as e:
            print(f"   ⚠️  Erro na circunscrição: {e}")
            df_input["fora_circunscricao"] = False
        
        return df_input

    def _reordenar_colunas(self, df_final):
        """Reordena colunas para clareza visual"""
        ordem_preferencial = [
            'id_sinistro', 'linha_origem', 'status_processamento', 'motivo_status',
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

    def _salvar_resultado(self, df_final, caminho_entrada, pasta_saida, sufixo, usar_timestamp):
        """Salva o resultado com relatório profissional"""
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
        
        # Criar cópia sem colunas internas para salvar
        df_para_salvar = df_final.copy()
        
        # Remover colunas internas
        colunas_para_remover = ['linha_origem', 'status_processamento', 'motivo_status']
        for col in colunas_para_remover:
            if col in df_para_salvar.columns:
                df_para_salvar.drop(columns=[col], inplace=True)
        
        # Salvar arquivo
        df_para_salvar.to_excel(caminho_saida, index=False)
        
        # Relatório profissional
        self._gerar_relatorio(df_final, caminho_entrada, caminho_saida)
        
        return caminho_saida

    def _gerar_relatorio(self, df_final, caminho_entrada, caminho_saida):
        """Gera relatório detalhado do processamento"""
        print(f"\n{'='*60}")
        print("📊 RELATÓRIO DE PROCESSAMENTO - v12.0")
        print(f"{'='*60}")
        print(f"📁 Arquivo processado: {os.path.basename(caminho_entrada)}")
        print(f"📈 Total de registros: {len(df_final):,}")
        
        # Estatísticas de status
        if 'status_processamento' in df_final.columns:
            status_counts = df_final['status_processamento'].value_counts()
            print(f"\n🎯 STATUS DO PROCESSAMENTO:")
            for status, count in status_counts.items():
                percentual = (count / len(df_final)) * 100
                print(f"   • {status}: {count:,} ({percentual:.1f}%)")
        
        # Estatísticas de circunscrição
        if 'fora_circunscricao' in df_final.columns:
            total_fora = df_final['fora_circunscricao'].sum()
            percentual_fora = (total_fora / len(df_final)) * 100
            print(f"\n📍 CIRCUNSCRIÇÃO:")
            print(f"   • Fora da circunscrição: {total_fora:,} ({percentual_fora:.1f}%)")
            print(f"   • Dentro da circunscrição: {len(df_final) - total_fora:,} ({100 - percentual_fora:.1f}%)")
        
        # Estatísticas de geocoding
        if 'latitude_geocode' in df_final.columns:
            geocode_sucesso = df_final['latitude_geocode'].notna().sum()
            percentual_geocode = (geocode_sucesso / len(df_final)) * 100
            print(f"\n🎯 GEOCODING:")
            print(f"   • Geocoding bem-sucedido: {geocode_sucesso:,} ({percentual_geocode:.1f}%)")
        
        # Métricas de qualidade
        if 'distancia_km' in df_final.columns:
            distancias_validas = df_final['distancia_km'].dropna()
            if len(distancias_validas) > 0:
                print(f"\n📏 MÉTRICAS DE QUALIDADE:")
                print(f"   • Distância média: {distancias_validas.mean():.2f} km")
                print(f"   • Registros ≤ 1km: {(distancias_validas <= 1.0).sum():,} ({(distancias_validas <= 1.0).sum()/len(distancias_validas)*100:.1f}%)")
        
        print(f"\n💾 ARQUIVO SALVO: {caminho_saida}")
        print(f"{'='*60}")

# Função de conveniência para uso rápido
def processar_sinistros(arquivos, pasta_entrada=None, pasta_saida=None, sufixo="_enriquecido", usar_timestamp=False):
    """
    Função rápida para processamento em Jupyter
    """
    processor = ProcessadorSinistros()
    return processor.processar(arquivos, pasta_entrada, pasta_saida, sufixo, usar_timestamp)