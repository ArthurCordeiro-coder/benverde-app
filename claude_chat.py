"""
claude_chat.py  (backend xAI / Grok)
Módulo de integração com a API xAI para o sistema de
gerenciamento de empresa hortifrúti.

Fluxo de dados:
    1. dados brutos (DataFrames) → montar_dados_para_llm() → JSON limpo
    2. pergunta simples de preço → buscar_preco_fallback() → resposta direta (sem LLM)
    3. demais perguntas → JSON + system prompt → xAI Grok → texto
"""

import os
import re
import json
import logging
from datetime import datetime
import time
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI, APIConnectionError, APIStatusError

from data_pipeline import montar_dados_para_llm, _norm_key

load_dotenv()

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes e cliente xAI
# ---------------------------------------------------------------------------
_MODELO       = "grok-4-1-fast-reasoning"
_MODELO_VISAO = "grok-2-vision-1212"

_cliente = OpenAI(
    api_key=os.environ.get("XAI_API_KEY", ""),
    base_url="https://api.x.ai/v1",
)

# Regex para detectar perguntas simples de preço
_RE_PERGUNTA_PRECO = re.compile(
    r"\b(pre[çc]o|quanto\s+(custa|est[áa]|[eé])|valor|custa)\b",
    re.IGNORECASE | re.UNICODE,
)

# ---------------------------------------------------------------------------
# Identidade permanente da Mita
# ---------------------------------------------------------------------------

# Bloco 1: quem ela é
_IDENTIDADE_MITA = (
    "Você é a Mita, gerente de dados da Benverde.\n"
    "A Benverde é uma empresa de hortifrúti especializada em frutas e vegetais, "
    "com foco estratégico em bananas.\n\n"
    "IDENTIDADE:\n"
    "- Nome: Mita\n"
    "- Cargo: Gerente de Dados da Benverde\n"
    "- Especialidade: gestão de hortifrúti, controle de estoque, "
    "análise de vendas e organização de dados operacionais\n\n"
    "PERSONALIDADE:\n"
    "- Gentil, amigável e levemente inocente — mas sempre profissional\n"
    "- Muito comprometida com a empresa\n"
    "- Proativa: destaca riscos e oportunidades sem esperar ser perguntada\n"
    "- Nunca robótica, nunca fria, nunca informal demais\n"
    "- Nunca exagerada ou infantil\n"
)

# Bloco 2: comportamento e formatação
_COMPORTAMENTO_MITA = (
    "COMPORTAMENTO OBRIGATÓRIO:\n"
    "- Responda SEMPRE em português do Brasil\n"
    "- Analise e interprete os dados — nunca liste dados brutos\n"
    "- Seja direta e objetiva — sem frases vagas como 'é importante monitorar'\n"
    "- Se faltar dados, informe claramente o que está faltando\n"
    "- Nunca invente dados\n\n"
    "CRITÉRIOS DE ANÁLISE (aplique sempre que houver dados):\n"
    "- Abaixo de 70% da meta → ALERTA DE BAIXO DESEMPENHO\n"
    "- Entre 70% e 110% → DENTRO DO ESPERADO\n"
    "- Acima de 110% → EXCESSO / POSSÍVEL SOBRA\n"
    "- Estoque baixo + meta alta → RISCO DE RUPTURA\n"
    "- Preço acima do concorrente → PERDA DE COMPETITIVIDADE\n"
    "- Preço abaixo do concorrente → OPORTUNIDADE DE GANHO DE MARGEM\n\n"
    "FORMATAÇÃO DE PREÇOS — REGRA ABSOLUTA:\n"
    "- CORRETO: R$ 4,89  (cifrão + espaço + vírgula decimal)\n"
    "- ERRADO: 4.89 R  /  R$4,89  /  16.5 R  /  R$ 4.89\n"
    "- Comparações: 'Semar R$ 4,89 vs Rossi R$ 7,26'\n"
    "- Nunca use LaTeX, fórmulas, itálico em números\n"
    "- Negrito apenas para títulos de seção\n"
)

# Bloco 3a: instrução de abertura — SOMENTE na primeira mensagem da conversa
_INSTRUCAO_PRIMEIRA_INTERACAO = (
    "INSTRUÇÃO DE ABERTURA — PRIMEIRA MENSAGEM DA CONVERSA:\n"
    "Esta é sua primeira resposta. Inicie OBRIGATORIAMENTE com exatamente:\n"
    "Oie! eu sou a Mita, sua gerente de dados da Benverde! como posso te ajudar hoje?\n"
    "Após a saudação, responda normalmente à pergunta.\n"
)

# Bloco 3b: instrução de continuidade — TODOS os turnos seguintes
_INSTRUCAO_CONTINUACAO = (
    "INSTRUÇÃO DE CONTINUIDADE:\n"
    "Esta conversa já está em andamento.\n"
    "NÃO repita a saudação inicial. Responda diretamente à pergunta.\n"
)

# Instrução injetada como segunda mensagem de sistema — reforça uso exclusivo do JSON
_INSTRUCAO_DADOS = (
    "Use EXCLUSIVAMENTE os dados do campo 'DADOS' para responder. "
    "Se a informação solicitada não estiver nos dados, informe claramente que não há registro. "
    "Formate preços como: R$ 4,89 (cifrão + espaço + vírgula decimal). "
    "Seja direta e objetiva."
)


def _system_prompt(primeira_interacao: bool = False) -> str:
    """Monta o system prompt completo da Mita.

    Args:
        primeira_interacao: True apenas no primeiro turno da conversa.
            Injeta a instrução de saudação obrigatória.
            False em todos os turnos seguintes — suprime a saudação.
    """
    instrucao_abertura = (
        _INSTRUCAO_PRIMEIRA_INTERACAO if primeira_interacao else _INSTRUCAO_CONTINUACAO
    )
    return f"{_IDENTIDADE_MITA}\n{_COMPORTAMENTO_MITA}\n{instrucao_abertura}"


# ---------------------------------------------------------------------------
# Fallback determinístico para consultas simples de preço
# ---------------------------------------------------------------------------

def _is_pergunta_preco(mensagem: str) -> bool:
    """Retorna True se a mensagem parece ser uma consulta simples de preço."""
    return bool(_RE_PERGUNTA_PRECO.search(mensagem))


def buscar_preco_fallback(mensagem: str, dados_raw: dict) -> Optional[str]:
    """Responde consultas de preço diretamente, sem chamar o LLM.

    Normaliza a mensagem, encontra o produto nos dados estruturados e
    retorna string formatada com os preços por loja.
    Retorna None se nenhum produto for identificado.
    """
    try:
        dados_llm = montar_dados_para_llm(
            precos_dict=dados_raw.get("precos", {}),
            metas_df=dados_raw.get("metas"),
            progresso_df=dados_raw.get("progresso"),
            saldo_estoque=dados_raw.get("saldo_estoque"),
            historico_estoque=dados_raw.get("historico_estoque"),
        )
    except Exception as exc:
        logger.warning("[fallback] Falha ao montar dados: %s", exc)
        return None

    precos = dados_llm.get("precos", {})
    if not precos:
        return None

    msg_norm = _norm_key(mensagem)

    # Encontra o produto cujas palavras significativas estão todas na mensagem
    for produto_key, lojas in precos.items():
        palavras = [p for p in produto_key.split() if len(p) > 3]
        if palavras and all(p in msg_norm for p in palavras):
            partes = [
                f"{loja} R$ {preco:.2f}".replace(".", ",")
                for loja, preco in lojas.items()
            ]
            data_ref = dados_llm.get("data_referencia", "")
            sufixo   = f" (ref. {data_ref})" if data_ref else ""
            return f"{produto_key.title()}{sufixo}: {' / '.join(partes)}"

    return None


# ---------------------------------------------------------------------------
# Wrapper da chamada à API xAI
# ---------------------------------------------------------------------------

def _chamar_xai(messages: list) -> str:
    """Envia lista de mensagens ao xAI Grok e retorna o texto da resposta.

    Raises:
        APIConnectionError, APIStatusError, Exception
    """
    resposta = _cliente.chat.completions.create(
        model=_MODELO,
        messages=messages,
        temperature=0.2,
        top_p=0.8,
        max_tokens=512,
    )
    return resposta.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Função principal: single-turn
# ---------------------------------------------------------------------------

def chat_com_grok(mensagem: str, dados: dict) -> str:
    """Envia uma pergunta ao modelo local com os dados da empresa em JSON limpo.

    Tenta primeiro o fallback determinístico para perguntas de preço.
    Só chama o LLM se o fallback não resolver.

    Args:
        mensagem: Pergunta do usuário.
        dados: Dict com chaves precos, metas, progresso, saldo_estoque,
               historico_estoque (formato de _dados_para_chat()).

    Returns:
        Texto da resposta.
    """
    # 1. Fallback determinístico para perguntas simples de preço
    if _is_pergunta_preco(mensagem):
        fallback = buscar_preco_fallback(mensagem, dados)
        if fallback:
            logger.info("[fallback] Resposta direta (sem LLM): %s", fallback[:80])
            return fallback

    # 2. Monta JSON limpo
    try:
        dados_llm = montar_dados_para_llm(
            precos_dict=dados.get("precos", {}),
            metas_df=dados.get("metas"),
            progresso_df=dados.get("progresso"),
            saldo_estoque=dados.get("saldo_estoque"),
            historico_estoque=dados.get("historico_estoque"),
        )
    except Exception as exc:
        logger.warning("Falha ao montar dados para LLM: %s", exc)
        dados_llm = {}

    dados_json_str = json.dumps(dados_llm, ensure_ascii=False)
    logger.info("Enviando pergunta ao xAI Grok: '%s'", mensagem[:80])
    logger.debug("DADOS ENVIADOS AO LLM:\n%s", json.dumps(dados_llm, indent=2, ensure_ascii=False))

    messages = [
        {"role": "system", "content": _system_prompt(primeira_interacao=True)},
        {"role": "system", "content": _INSTRUCAO_DADOS},
        {"role": "user",   "content": f"DADOS:{dados_json_str}\n\nPERGUNTA: {mensagem.strip()}"},
    ]

    try:
        texto = _chamar_xai(messages)
        logger.info("Resposta recebida (%d chars)", len(texto))
        return texto or "A IA retornou uma resposta vazia."

    except APIConnectionError as exc:
        logger.error("Falha de conexão com o xAI: %s", exc)
        return "Erro na IA: sem conexão com o servidor xAI. Verifique sua conexão."

    except APIStatusError as exc:
        logger.error("Erro HTTP do xAI: %s", exc)
        return "Erro na IA, tente depois."

    except Exception as exc:
        logger.error("Erro inesperado ao chamar o xAI: %s", exc)
        return "Erro na IA, tente depois."


# ---------------------------------------------------------------------------
# Função auxiliar para uso em Streamlit: multi-turno com histórico
# ---------------------------------------------------------------------------

def chat_com_grok_historico(
    mensagem: str,
    dados: dict,
    historico_chat: list[dict],
) -> tuple[str, list[dict]]:
    """Versão com histórico de conversa multi-turno para uso em Streamlit.

    Dados da empresa são injetados como JSON apenas no primeiro turno.
    Turnos seguintes reutilizam o contexto já presente no histórico da API.

    Args:
        mensagem: Nova pergunta do usuário.
        dados: Dict com dados brutos da empresa.
        historico_chat: Lista de mensagens anteriores user/assistant (sem system).
                        Passar [] na primeira chamada da sessão.

    Returns:
        Tupla (resposta_str, historico_atualizado).
    """
    # 1. Fallback determinístico para perguntas simples de preço
    if _is_pergunta_preco(mensagem):
        fallback = buscar_preco_fallback(mensagem, dados)
        if fallback:
            logger.info("[fallback] Resposta direta (sem LLM): %s", fallback[:80])
            historico_atualizado = historico_chat + [
                {"role": "user",      "content": mensagem},
                {"role": "assistant", "content": fallback},
            ]
            return fallback, historico_atualizado

    # 2. Detecta primeiro turno para controle de saudação e injeção de dados
    eh_primeira_interacao = not historico_chat

    if eh_primeira_interacao:
        # Injeta JSON completo apenas na primeira mensagem
        try:
            dados_llm = montar_dados_para_llm(
                precos_dict=dados.get("precos", {}),
                metas_df=dados.get("metas"),
                progresso_df=dados.get("progresso"),
                saldo_estoque=dados.get("saldo_estoque"),
                historico_estoque=dados.get("historico_estoque"),
            )
        except Exception as exc:
            logger.warning("Falha ao montar dados para LLM: %s", exc)
            dados_llm = {}

        dados_json_str = json.dumps(dados_llm, ensure_ascii=False)
        logger.debug(
            "DADOS ENVIADOS AO LLM:\n%s",
            json.dumps(dados_llm, indent=2, ensure_ascii=False),
        )
        conteudo_user = f"DADOS:{dados_json_str}\n\nPERGUNTA: {mensagem.strip()}"
    else:
        # Turnos seguintes: apenas a pergunta — dados já estão no histórico
        conteudo_user = f"PERGUNTA: {mensagem.strip()}"

    historico_novo = historico_chat + [{"role": "user", "content": conteudo_user}]

    mensagens_api = [
        {"role": "system", "content": _system_prompt(primeira_interacao=eh_primeira_interacao)},
        {"role": "system", "content": _INSTRUCAO_DADOS},
    ] + historico_novo

    logger.info("[histórico] Pergunta: '%s' | turns=%d", mensagem[:60], len(historico_chat) // 2)

    try:
        texto = _chamar_xai(mensagens_api)
        historico_atualizado = historico_novo + [{"role": "assistant", "content": texto}]
        logger.info("[histórico] Resposta (%d chars)", len(texto))
        return texto or "Resposta vazia.", historico_atualizado

    except APIConnectionError as exc:
        logger.error("Falha de conexão com o xAI: %s", exc)
        return "Erro na IA: sem conexão com o servidor xAI. Verifique sua conexão.", historico_chat

    except APIStatusError as exc:
        logger.error("Erro HTTP do xAI: %s", exc)
        return "Erro na IA, tente depois.", historico_chat

    except Exception as exc:
        logger.error("Erro inesperado ao chamar o xAI: %s", exc)
        return "Erro na IA, tente depois.", historico_chat


# ---------------------------------------------------------------------------
# Extração de metas via imagem (não usa pipeline de dados)
# ---------------------------------------------------------------------------

def extrair_metas_de_imagem(imagem_bytes: bytes, mime_type: str = "image/png") -> list[dict]:
    """Envia imagem de tabela de metas para o modelo local e retorna lista estruturada.

    Args:
        imagem_bytes: Conteúdo binário da imagem (PNG, JPEG, etc.).
        mime_type:    Tipo MIME da imagem (mantido para compatibilidade de interface).

    Returns:
        Lista de dicts ``[{"Produto": str, "Meta": int}, ...]``.
        Retorna lista vazia em caso de falha.
    """
    import base64

    imagem_b64 = base64.standard_b64encode(imagem_bytes).decode("utf-8")

    prompt = (
        "Você receberá a imagem de uma tabela de metas de vendas de uma empresa hortifrúti.\n"
        "Extraia TODOS os produtos e suas respectivas metas (quantidades numéricas).\n\n"
        "Retorne SOMENTE um JSON válido, sem nenhum texto antes ou depois, "
        "sem markdown, sem explicações. Formato exato:\n"
        '[{"Produto": "NOME DO PRODUTO", "Meta": 123}, ...]\n\n'
        "Regras:\n"
        "- Nome do produto: sempre em MAIÚSCULAS\n"
        "- Meta: sempre número inteiro (arredonde se necessário)\n"
        "- Se uma célula estiver vazia ou ilegível, ignore a linha\n"
        "- Não inclua linhas de totais ou cabeçalhos"
    )

    try:
        resposta = _cliente.chat.completions.create(
            model=_MODELO_VISAO,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:{mime_type};base64,{imagem_b64}"},
                        },
                    ],
                }
            ],
            temperature=0.0,
        )

        texto = resposta.choices[0].message.content.strip()
        texto = re.sub(r"^```(?:json)?\s*", "", texto)
        texto = re.sub(r"\s*```$",          "", texto)

        metas = json.loads(texto)

        resultado = []
        for item in metas:
            produto = str(item.get("Produto", "")).strip().upper()
            try:
                meta = int(float(str(item.get("Meta", 0)).replace(",", ".")))
            except (ValueError, TypeError):
                meta = 0
            if produto and meta > 0:
                resultado.append({"Produto": produto, "Meta": meta})

        logger.info("Metas extraídas da imagem: %d produto(s).", len(resultado))
        return resultado

    except APIConnectionError as exc:
        logger.error("Falha de conexão com o xAI: %s", exc)
        return []
    except json.JSONDecodeError as exc:
        logger.error("Resposta do modelo não é JSON válido: %s", exc)
        return []
    except Exception as exc:
        logger.error("Erro ao extrair metas da imagem: %s", exc)
        return []


def extrair_metas_de_planilha(df) -> list[dict]:
    """Envia tabela xlsx como texto para o modelo e retorna lista estruturada.

    A IA identifica as colunas de produto e meta e corrige erros de ortografia
    nos nomes dos produtos (ex: 'BANABA' → 'BANANA').

    Args:
        df: DataFrame pandas com os dados da planilha.

    Returns:
        Lista de dicts ``[{"Produto": str, "Meta": int}, ...]``.
        Retorna lista vazia em caso de falha.
    """
    tabela_texto = df.to_csv(index=False, sep=";")

    prompt = (
        "Você receberá uma tabela CSV com dados de metas de vendas de uma empresa hortifrúti.\n"
        "Identifique quais colunas contêm os nomes dos produtos e as metas (quantidades numéricas).\n"
        "Corrija erros de ortografia nos nomes dos produtos em português "
        "(ex: 'BANABA' → 'BANANA', 'MACÃ' → 'MAÇÃ', 'ABOBRA' → 'ABÓBORA').\n\n"
        "Retorne SOMENTE um JSON válido, sem nenhum texto antes ou depois, "
        "sem markdown, sem explicações. Formato exato:\n"
        '[{"Produto": "NOME DO PRODUTO", "Meta": 123}, ...]\n\n'
        "Regras:\n"
        "- Nome do produto: sempre em MAIÚSCULAS, ortografia corrigida\n"
        "- Meta: sempre número inteiro (arredonde se necessário)\n"
        "- Se uma célula estiver vazia ou ilegível, ignore a linha\n"
        "- Não inclua linhas de totais ou cabeçalhos\n\n"
        f"Tabela:\n{tabela_texto}"
    )

    try:
        resposta = _cliente.chat.completions.create(
            model=_MODELO,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )

        texto = resposta.choices[0].message.content.strip()
        texto = re.sub(r"^```(?:json)?\s*", "", texto)
        texto = re.sub(r"\s*```$",          "", texto)

        metas = json.loads(texto)

        resultado = []
        for item in metas:
            produto = str(item.get("Produto", "")).strip().upper()
            try:
                meta = int(float(str(item.get("Meta", 0)).replace(",", ".")))
            except (ValueError, TypeError):
                meta = 0
            if produto and meta > 0:
                resultado.append({"Produto": produto, "Meta": meta})

        logger.info("Metas extraídas da planilha: %d produto(s).", len(resultado))
        return resultado

    except APIConnectionError as exc:
        logger.error("Falha de conexão com o xAI: %s", exc)
        return []
    except json.JSONDecodeError as exc:
        logger.error("Resposta do modelo não é JSON válido: %s", exc)
        return []
    except Exception as exc:
        logger.error("Erro ao extrair metas da planilha: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Bloco de testes com dados mock
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pandas as pd

    print("=" * 60)
    print("TESTE: claude_chat.py  →  xAI / grok-4-1-fast-reasoning  |  dados mock")
    print("=" * 60)

    df_preco_hoje = pd.DataFrame({
        "Produto Buscado": ["BANANA NANICA", "BANANA DA TERRA", "BANANA PRATA"],
        "Preço (Semar)":   [3.49, 4.99, 5.29],
        "Status (Semar)":  ["OK", "OK", "OK"],
        "Preço (Rossi)":   [3.59, 5.10, 5.50],
        "Status (Rossi)":  ["OK", "OK", "Indisponível"],
    })
    df_preco_ontem = pd.DataFrame({
        "Produto Buscado": ["BANANA NANICA", "BANANA DA TERRA"],
        "Preço (Semar)":   [3.45, 4.90],
        "Status (Semar)":  ["OK", "OK"],
        "Preço (Rossi)":   [3.55, 5.00],
        "Status (Rossi)":  ["OK", "OK"],
    })
    mock_precos = {"23-02-2026": df_preco_hoje, "22-02-2026": df_preco_ontem}

    mock_progresso = pd.DataFrame({
        "Produtos":       ["BANANA NANICA", "BANANA DA TERRA", "BANANA PRATA"],
        "meta":           [500, 300, 200],
        "pedido":         [420, 280, 160],
        "Progresso":      [84.0, 93.3, 80.0],
        "status da meta": ["META EM ANDAMENTO"] * 3,
    })

    # colunas reais retornadas por load_metas_local
    mock_metas = pd.DataFrame({
        "Produto": ["BANANA NANICA", "BANANA DA TERRA", "BANANA PRATA"],
        "Meta":    [500, 300, 200],
    })

    mock_saldo = 248.0
    mock_historico = [
        {"data": datetime(2026, 2, 22), "tipo": "saida",   "produto": "BANANA NANICA",
         "quant": 52.0,  "unidade": "KG", "valor_unit": 3.49, "valor_total": 181.5},
        {"data": datetime(2026, 2, 23), "tipo": "entrada", "produto": "BANANA PRATA",
         "quant": 130.0, "unidade": "KG", "valor_unit": 4.80, "valor_total": 624.0},
    ]

    mock_dados = {
        "precos":            mock_precos,
        "progresso":         mock_progresso,
        "pedidos":           None,
        "metas":             mock_metas,
        "saldo_estoque":     mock_saldo,
        "historico_estoque": mock_historico,
    }

    # Testa fallback
    print("\n--- TESTE FALLBACK ---")
    resp = buscar_preco_fallback("quanto está o preço da banana nanica?", mock_dados)
    print(f"Fallback: {resp}")

    # Testa single-turn
    print("\n--- TESTE SINGLE-TURN ---")
    for pergunta in [
        "Qual o preço da banana nanica?",
        "Como está o progresso das metas?",
    ]:
        print(f"\nPergunta: {pergunta}")
        print(f"Mita: {chat_com_grok(pergunta, mock_dados)}")
        time.sleep(1)

    # Testa multi-turno
    print("\n--- TESTE MULTI-TURNO ---")
    hist: list[dict] = []
    for pergunta in ["Me dê um resumo do estoque", "E as metas, como estão?"]:
        print(f"\nUsuário: {pergunta}")
        resposta, hist = chat_com_grok_historico(pergunta, mock_dados, hist)
        print(f"Mita: {resposta}")

    print(f"\nTurns no histórico: {len(hist) // 2}")
