"""
Módulo para normalização numérica em endereços
"""

import re
import pandas as pd
import logging

def _gerar_numero_por_extenso(numero: int) -> str:
    """
    Gera a representação por extenso de qualquer número até 999
    """
    if numero < 0 or numero > 999:
        return str(numero)
    
    # Caso especial: 100
    if numero == 100:
        return 'CEM'
    
    # Caso especial: 0 (não deve ocorrer em endereços, mas por segurança)
    if numero == 0:
        return 'ZERO'
    
    unidades = ['', 'UM', 'DOIS', 'TRES', 'QUATRO', 'CINCO', 'SEIS', 'SETE', 'OITO', 'NOVE']
    especiais = ['DEZ', 'ONZE', 'DOZE', 'TREZE', 'QUATORZE', 'QUINZE', 'DEZESSEIS', 'DEZESSETE', 'DEZOITO', 'DEZENOVE']
    dezenas = ['', '', 'VINTE', 'TRINTA', 'QUARENTA', 'CINQUENTA', 'SESSENTA', 'SETENTA', 'OITENTA', 'NOVENTA']
    centenas = ['', 'CENTO', 'DUZENTOS', 'TREZENTOS', 'QUATROCENTOS', 'QUINHENTOS', 'SEISCENTOS', 'SETECENTOS', 'OITOCENTOS', 'NOVECENTOS']
    
    # Separa centenas, dezenas e unidades
    c = numero // 100
    d = (numero % 100) // 10
    u = numero % 10
    
    partes = []
    
    # Centenas
    if c > 0:
        partes.append(centenas[c])
    
    # Dezenas e unidades
    resto = numero % 100
    if resto > 0:
        if resto < 10:
            partes.append(unidades[resto])
        elif resto < 20:
            partes.append(especiais[resto - 10])
        else:
            partes.append(dezenas[d])
            if u > 0:
                partes.append('E')
                partes.append(unidades[u])
    
    return ' '.join(partes)

def _normalizar_numeros_por_extenso(texto: str) -> str:
    """
    Normaliza números escritos em algarismos para por extenso
    Exemplo: 'TUNEL 9 DE JULHO' → 'TUNEL NOVE DE JULHO'
    'RUA 156' → 'RUA CENTO E CINQUENTA E SEIS'
    """
    if not texto or pd.isna(texto):
        return texto
    
    texto_upper = texto.upper().strip()
    
    # Padrões comuns de datas e números em endereços (MAIS ABRANGENTE)
    padroes_especificos = {
        r'\b1\s*º?\s*DE\s*MAIO\b': 'PRIMEIRO DE MAIO',
        r'\b7\s*DE\s*SETEMBRO\b': 'SETE DE SETEMBRO', 
        r'\b9\s*DE\s*JULHO\b': 'NOVE DE JULHO',
        r'\b15\s*DE\s*NOVEMBRO\b': 'QUINZE DE NOVEMBRO',
        r'\b25\s*DE\s*MARCO\b': 'VINTE E CINCO DE MARCO',
        r'\b24\s*DE\s*MAIO\b': 'VINTE E QUATRO DE MAIO',
        r'\b13\s*DE\s*MAIO\b': 'TREZE DE MAIO',
        r'\b28\s*DE\s*SETEMBRO\b': 'VINTE E OITO DE SETEMBRO',
        r'\b1\s*º?\s*DE\s*ABRIL\b': 'PRIMEIRO DE ABRIL',
        r'\b21\s*DE\s*ABRIL\b': 'VINTE E UM DE ABRIL',
    }
    
    # Mapeamento de ordinais especiais
    ordinais_especiais = {
        '1': 'PRIMEIRO', '2': 'SEGUNDO', '3': 'TERCEIRO', '4': 'QUARTO',
        '5': 'QUINTO', '6': 'SEXTO', '7': 'SETIMO', '8': 'OITAVO',
        '9': 'NONO', '10': 'DECIMO'
    }
    
    # Primeiro aplica os padrões específicos (mais precisos)
    texto_normalizado = texto_upper
    for padrao, substituicao in padroes_especificos.items():
        texto_normalizado = re.sub(padrao, substituicao, texto_normalizado, flags=re.IGNORECASE)
    
    # Função para substituir números no contexto
    def substituir_numero(match):
        numero_str = match.group(1)
        contexto_ante = match.group(2) if match.group(2) else ""
        contexto_post = match.group(3) if match.group(3) else ""
        
        try:
            numero = int(numero_str)
            
            # Só converte números em contextos específicos de endereços
            contextos_validos = [
                ' DE ', 'TUNEL', 'PONTE', 'VIADUTO', 'RUA', 'AVENIDA', 'ALAMEDA',
                'PRACA', 'LARGO', 'TRAVESSA', 'BECO', 'VIA', 'ELEVADO', 'PARQUE'
            ]
            
            contexto_relevante = any(ctx in contexto_ante.upper() or ctx in contexto_post.upper() 
                                   for ctx in contextos_validos)
            
            if contexto_relevante and 1 <= numero <= 999:
                # Verifica se é ordinal (termina com º)
                if match.group(0).endswith('º'):
                    if numero_str in ordinais_especiais:
                        return ordinais_especiais[numero_str]
                    else:
                        return _gerar_numero_por_extenso(numero)  # Fallback
                else:
                    return _gerar_numero_por_extenso(numero)
            else:
                return match.group(0)  # Mantém o número original
                
        except ValueError:
            return match.group(0)  # Mantém se não for número válido
    
    # Padrão para capturar números em contextos de endereço
    # Captura: (palavras antes) + número + (palavras depois)
    padrao_numeros = r'(\b\w+\s+)*\b(\d{1,3})º?\b(\s+\w+)*'
    
    # Aplica a substituição
    texto_final = re.sub(padrao_numeros, substituir_numero, texto_normalizado)
    
    # Log apenas se houve alteração significativa
    if texto_final != texto_upper:
        numeros_alterados = re.findall(r'\b\d{1,3}\b', texto_upper)
        if numeros_alterados:
            logging.info(f"🔤 Normalização numérica: '{texto}' -> '{texto_final}'")
    
    return texto_final