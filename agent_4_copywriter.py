"""
AGENTE 4: COPY GENERATOR
SalesMachine v4.0

Responsabilidades:
- Receber lead enriquecido do Agente 3
- Analisar contexto (idade, maturidade, canais disponíveis)
- Gerar copies personalizadas via Gemini
- Gerar copies genéricas se não tiver contatos
- Enviar copies para aprovação via Telegram
"""

import os
import json
import requests
import datetime
import traceback
import re
import google.generativeai as genai
from google.cloud import pubsub_v1
from google.cloud import firestore
from dotenv import load_dotenv

# --- Banco ---
try:
    import database
    print("✅ [Agente 4] Módulo database carregado.")
except ImportError:
    print("⚠️ [Agente 4] ERRO: database.py não encontrado.")
    class database:
        @staticmethod
        def update_copies(domain, data): pass
        @staticmethod
        def save_debug_log(a, b, c, d=None): pass

load_dotenv()
print("\n✍️ --- AGENTE 4: COPY GENERATOR (V1.0 - Gemini Powered) ---")

# --- Configurações ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DEBUG_CHAT_ID = os.getenv("DEBUG_CHAT_ID", "-1002424609562")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

SUBSCRIPTION_INPUT = "sub-copy-generator"
MODELO_GEMINI = "gemini-2.0-flash-lite-preview-02-05"

# --- GCP Setup ---
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_INPUT)
db_firestore = firestore.Client(project=PROJECT_ID)

# --- Gemini Setup ---
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    print("✅ Gemini configurado!")
else:
    print("❌ ERRO: GEMINI_API_KEY não encontrada!")

# ==============================================================================
# 🎯 MATRIZ DE TOM
# ==============================================================================

"""
Matriz de Tom (Idade x Maturidade):

                    EMPRESA JOVEM           EMPRESA MADURA
                    (< 5 anos ou            (>= 5 anos ou
                     stack moderna)          stack tradicional)
                     
CONTATO JOVEM       🚀 DIRETO               💼 CONSULTIVO
(até 40 anos)       Informal, emojis        Profissional, dados
                    "E aí, tudo bem?"       "Boa tarde!"

CONTATO SÊNIOR      💡 INOVADOR             🎩 FORMAL
(> 40 anos)         Respeitoso + moderno    Tradicional, formal
                    "Sr. Roberto..."        "Prezado Sr..."
"""

IDADE_MAP = {
    "entre 21 a 30 anos": "jovem",
    "entre 31 a 40 anos": "jovem",
    "entre 41 a 50 anos": "senior",
    "entre 51 a 60 anos": "senior",
    "entre 61 a 70 anos": "senior",
    "maior de 70 anos": "senior",
}


def get_tom_copy(faixa_etaria, stack_maturity, founded_year=None):
    """
    Determina o tom da copy baseado na idade do contato e maturidade da empresa.
    Retorna: 'direto', 'consultivo', 'inovador', 'formal'
    """
    idade_contato = "jovem"  # Default
    if faixa_etaria:
        idade_contato = IDADE_MAP.get(faixa_etaria.lower(), "jovem")
    
    empresa_jovem = True  # Default
    if stack_maturity == "traditional":
        empresa_jovem = False
    elif founded_year:
        try:
            anos_empresa = datetime.datetime.now().year - int(founded_year)
            empresa_jovem = anos_empresa < 5
        except:
            pass
    
    # Matriz de decisão
    if idade_contato == "jovem" and empresa_jovem:
        return "direto"
    elif idade_contato == "jovem" and not empresa_jovem:
        return "consultivo"
    elif idade_contato == "senior" and empresa_jovem:
        return "inovador"
    else:
        return "formal"


# ==============================================================================
# 📝 PROMPTS POR TOM
# ==============================================================================

PROMPTS_TOM = {
    "direto": """
Você é um SDR jovem e dinâmico. Escreva uma copy {canal} para {nome}.

CONTEXTO:
- Empresa: {empresa}
- Cargo: {cargo}
- Tecnologias: {techs}

ESTILO:
- Tom informal e direto
- Pode usar 1-2 emojis estratégicos
- Frases curtas e impactantes
- Comece com algo como "E aí" ou "Opa"
- Foque em resultados rápidos

REGRAS:
- Máximo 3 parágrafos curtos
- Inclua uma pergunta no final
- Não seja genérico, mencione algo específico da empresa
""",

    "consultivo": """
Você é um consultor de negócios experiente. Escreva uma copy {canal} para {nome}.

CONTEXTO:
- Empresa: {empresa}
- Cargo: {cargo}
- Tecnologias: {techs}

ESTILO:
- Tom profissional mas acessível
- Foque em dados e resultados
- Mencione cases ou números quando possível
- Comece com "Boa tarde" ou "Olá"
- Posicione-se como especialista

REGRAS:
- Máximo 4 parágrafos
- Inclua um insight sobre o mercado deles
- Termine com proposta de valor clara
""",

    "inovador": """
Você é um executivo que entende de inovação. Escreva uma copy {canal} para {nome}.

CONTEXTO:
- Empresa: {empresa}
- Cargo: {cargo}
- Tecnologias: {techs}

ESTILO:
- Tom respeitoso mas com visão moderna
- Use "Sr./Sra." no início
- Fale sobre transformação digital
- Mencione tendências do setor
- Mostre que entende os desafios de liderança

REGRAS:
- Máximo 4 parágrafos
- Seja respeitoso mas não formal demais
- Foque em visão estratégica
""",

    "formal": """
Você é um diretor comercial experiente. Escreva uma copy {canal} para {nome}.

CONTEXTO:
- Empresa: {empresa}
- Cargo: {cargo}
- Tecnologias: {techs}

ESTILO:
- Tom formal e respeitoso
- Use "Prezado Sr./Sra."
- Linguagem corporativa
- Foque em credibilidade e track record
- Evite gírias ou emojis

REGRAS:
- Máximo 4 parágrafos
- Seja direto ao ponto
- Mencione credenciais ou cases relevantes
"""
}

# Variações por canal
CANAL_SPECS = {
    "email": {
        "max_chars": 1500,
        "incluir": "Assunto do email (linha separada no início)",
        "formato": "Email profissional com saudação e assinatura"
    },
    "linkedin": {
        "max_chars": 300,
        "incluir": "Pedido de conexão breve",
        "formato": "Mensagem curta e direta para InMail/conexão"
    },
    "whatsapp": {
        "max_chars": 500,
        "incluir": "Pode usar emojis com moderação",
        "formato": "Mensagem conversacional, como se fosse para um conhecido profissional"
    }
}


# ==============================================================================
# 🤖 GERAÇÃO DE COPY VIA GEMINI
# ==============================================================================

def generate_copy_gemini(prompt):
    """Gera copy usando Gemini"""
    try:
        model = genai.GenerativeModel(MODELO_GEMINI)
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"   ⚠️ Erro Gemini: {e}")
        return None


def build_copy_prompt(contact, company_name, tech_summary, tom, canal, is_generic=False):
    """Constrói o prompt para geração de copy"""
    
    nome = contact.get("name", "")
    cargo = contact.get("title", "Decisor")
    
    # Se for contato genérico, ajusta
    if is_generic or contact.get("is_generic"):
        nome = f"Equipe {company_name}"
        cargo = "Equipe"
        tom = "consultivo"  # Força tom consultivo para genéricos
    
    techs = []
    if tech_summary:
        if tech_summary.get("marketing"):
            techs.extend(tech_summary["marketing"][:2])
        if tech_summary.get("cms"):
            techs.extend(tech_summary["cms"][:1])
        if tech_summary.get("ecommerce"):
            techs.extend(tech_summary["ecommerce"][:1])
    
    techs_str = ", ".join(techs) if techs else "não identificadas"
    
    # Pega o template do tom
    base_prompt = PROMPTS_TOM.get(tom, PROMPTS_TOM["consultivo"])
    
    # Adiciona specs do canal
    canal_spec = CANAL_SPECS.get(canal, CANAL_SPECS["email"])
    
    prompt = base_prompt.format(
        canal=canal,
        nome=nome,
        empresa=company_name,
        cargo=cargo,
        techs=techs_str
    )
    
    prompt += f"""

ESPECIFICAÇÕES DO CANAL ({canal.upper()}):
- Máximo {canal_spec['max_chars']} caracteres
- {canal_spec['incluir']}
- Formato: {canal_spec['formato']}

Responda APENAS com a copy, sem explicações.
"""
    
    return prompt


def generate_copies_for_contact(contact, company_name, tech_summary, stack_maturity):
    """Gera copies para todos os canais disponíveis de um contato"""
    
    copies = []
    is_generic = contact.get("is_generic", False)
    
    # Determina tom
    faixa_etaria = contact.get("faixa_etaria")
    tom = get_tom_copy(faixa_etaria, stack_maturity)
    
    # Se for genérico, força consultivo
    if is_generic:
        tom = "consultivo"
    
    # Email (sempre tenta se tiver email)
    email = contact.get("email")
    if email or is_generic:
        prompt = build_copy_prompt(contact, company_name, tech_summary, tom, "email", is_generic)
        copy_text = generate_copy_gemini(prompt)
        if copy_text:
            copies.append({
                "canal": "email",
                "destinatario": email or "contato@" + company_name.lower().replace(" ", "") + ".com.br",
                "copy": copy_text,
                "tom": tom,
                "is_generic": is_generic
            })
    
    # LinkedIn (sempre tenta se tiver linkedin ou for genérico)
    linkedin = contact.get("linkedin")
    if linkedin or is_generic:
        prompt = build_copy_prompt(contact, company_name, tech_summary, tom, "linkedin", is_generic)
        copy_text = generate_copy_gemini(prompt)
        if copy_text:
            copies.append({
                "canal": "linkedin",
                "destinatario": linkedin or f"LinkedIn da {company_name}",
                "copy": copy_text,
                "tom": tom,
                "is_generic": is_generic
            })
    
    # WhatsApp (só se tiver telefone E não for genérico)
    phone = contact.get("phone")
    if phone and not is_generic:
        prompt = build_copy_prompt(contact, company_name, tech_summary, tom, "whatsapp", is_generic)
        copy_text = generate_copy_gemini(prompt)
        if copy_text:
            copies.append({
                "canal": "whatsapp",
                "destinatario": phone,
                "copy": copy_text,
                "tom": tom,
                "is_generic": is_generic
            })
    
    return copies


# ==============================================================================
# 📱 TELEGRAM
# ==============================================================================

def send_telegram(chat_id, text):
    """Envia mensagem para o Telegram"""
    if not TELEGRAM_TOKEN or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            payload["parse_mode"] = None
            r = requests.post(url, json=payload, timeout=10)
        return r.json().get("result", {}).get("message_id") if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ Erro Telegram: {e}")
        return None


def clean_markdown(text):
    """Remove caracteres que quebram o Markdown"""
    if not text:
        return ""
    return text.replace("_", " ").replace("*", " ").replace("`", "'").replace("[", "(").replace("]", ")")


def format_copies_message(domain, company_name, all_copies):
    """Formata mensagem com todas as copies geradas"""
    
    msg = f"✍️ *COPIES GERADAS*\n"
    msg += f"🏢 {clean_markdown(company_name)}\n"
    msg += f"🌐 {domain}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Agrupa por contato
    contacts_copies = {}
    for copy in all_copies:
        dest = copy.get("destinatario", "N/D")
        if dest not in contacts_copies:
            contacts_copies[dest] = []
        contacts_copies[dest].append(copy)
    
    for idx, (destinatario, copies) in enumerate(contacts_copies.items()):
        # Verifica se é genérico
        is_generic = copies[0].get("is_generic", False) if copies else False
        
        if is_generic:
            msg += f"🏢 *Equipe {clean_markdown(company_name)}* _(contato genérico)_\n"
        else:
            msg += f"👤 *{clean_markdown(destinatario)}*\n"
        
        for copy in copies:
            canal = copy.get("canal", "").upper()
            tom = copy.get("tom", "")
            texto = copy.get("copy", "")
            
            emoji_canal = {"EMAIL": "📧", "LINKEDIN": "🔗", "WHATSAPP": "📱"}.get(canal, "📝")
            
            msg += f"\n{emoji_canal} *{canal}* (tom: {tom})\n"
            msg += f"```\n{texto[:500]}{'...' if len(texto) > 500 else ''}\n```\n"
        
        msg += "\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"✅ Total: {len(all_copies)} copies geradas"
    
    return msg


# ==============================================================================
# 🔄 PROCESSAMENTO PRINCIPAL
# ==============================================================================

def process_copy_request(data):
    """Processa requisição de geração de copies"""
    
    domain = data.get("domain")
    company_name = data.get("company_name", domain)
    contacts = data.get("contacts", [])
    tech_summary = data.get("tech_summary", {})
    chat_id = data.get("chat_id")
    stack_maturity = data.get("stack_maturity", "unknown")
    site_emails = data.get("site_emails", [])
    brasil_api_data = data.get("brasil_api_data", {})
    site_socials = data.get("site_socials", [])
    
    print(f"\n✍️ [Copy Request] Processando: {domain}")
    print(f"   📊 Contatos: {len(contacts)} | Stack: {stack_maturity}")
    
    # Se não tem contatos, cria contato genérico
    if not contacts:
        print(f"   ⚠️ Sem contatos. Gerando copy genérica...")
        
        # Tenta pegar email do site ou do CNPJ
        generic_email = None
        if site_emails:
            generic_email = site_emails[0]
        elif brasil_api_data and brasil_api_data.get("email"):
            generic_email = brasil_api_data.get("email")
        
        # Tenta pegar LinkedIn da empresa
        generic_linkedin = None
        for social in site_socials:
            if "linkedin" in social.lower():
                # Extrai URL
                match = re.search(r'https?://[^\s]+linkedin[^\s]+', social)
                if match:
                    generic_linkedin = match.group(0)
                    break
        
        contacts = [{
            "name": company_name,
            "email": generic_email,
            "linkedin": generic_linkedin,
            "is_generic": True
        }]
    
    all_copies = []
    
    for contact in contacts[:5]:  # Máximo 5 contatos
        name = contact.get("name", "Contato")
        print(f"   👤 Gerando para: {name}")
        
        copies = generate_copies_for_contact(
            contact=contact,
            company_name=company_name,
            tech_summary=tech_summary,
            stack_maturity=stack_maturity
        )
        
        for copy in copies:
            copy["contact_name"] = name
            all_copies.append(copy)
    
    print(f"   ✅ {len(all_copies)} copies geradas")
    
    # Salva no banco
    database.update_copies(domain, {
        "copies": all_copies,
        "generated_at": datetime.datetime.now().isoformat()
    })
    
    # Envia para Telegram
    if all_copies:
        msg = format_copies_message(domain, company_name, all_copies)
        
        # Quebra em múltiplas mensagens se muito grande
        if len(msg) > 4000:
            # Envia resumo primeiro
            resumo = f"✍️ *COPIES GERADAS*\n"
            resumo += f"🏢 {clean_markdown(company_name)}\n"
            resumo += f"🌐 {domain}\n"
            resumo += f"━━━━━━━━━━━━━━━━━━━━\n"
            resumo += f"✅ {len(all_copies)} copies geradas para {len(contacts)} contatos\n\n"
            resumo += "Copies salvas no banco. Use o dashboard para visualizar."
            send_telegram(chat_id, resumo)
        else:
            send_telegram(chat_id, msg)
    else:
        send_telegram(chat_id, f"❌ Não foi possível gerar copies para {domain}")
    
    return all_copies


# ==============================================================================
# 📨 CALLBACK PRINCIPAL
# ==============================================================================

def callback(message):
    """Callback principal do Pub/Sub"""
    try:
        data = json.loads(message.data.decode("utf-8"))
        domain = data.get("domain")
        
        print(f"\n📨 RECEBIDO: {domain}")
        
        # Debug
        database.save_debug_log("agent_4", "RECEIVED", {
            "domain": domain,
            "contacts_count": len(data.get("contacts", [])),
            "has_tech_summary": bool(data.get("tech_summary"))
        }, domain)
        
        # Processa
        process_copy_request(data)
        
        message.ack()
        
    except Exception as e:
        print(f"🔥 Erro Geral: {e}")
        traceback.print_exc()
        message.nack()


# ==============================================================================
# 🚀 MAIN
# ==============================================================================

if __name__ == "__main__":
    if not GEMINI_API_KEY:
        print("❌ ERRO: GEMINI_API_KEY não encontrada!")
        exit(1)
    
    print(f"\n📡 Debug Chat: {DEBUG_CHAT_ID}")
    print(f"🤖 Modelo: {MODELO_GEMINI}")
    
    flow_control = pubsub_v1.types.FlowControl(max_messages=2)
    print(f"\n✍️ Agente 4 (V1.0 - Gemini Powered) ouvindo: {SUBSCRIPTION_INPUT}")
    
    with subscriber:
        try:
            subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control).result()
        except KeyboardInterrupt:
            pass
