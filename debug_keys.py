import os
from dotenv import load_dotenv

# Força recarregar o arquivo .env agora
load_dotenv(override=True)

print("\n🔍 --- DIAGNÓSTICO DE CHAVES ---")
print(f"📂 Diretório onde estou rodando: {os.getcwd()}")
print(f"📄 O arquivo .env existe aqui? {'SIM' if os.path.exists('.env') else 'NÃO'}")

# Tenta ler a chave
apollo_key = os.getenv("APOLLO_API_KEY")

if apollo_key:
    # Mostra se a chave está vazia ou tem conteúdo
    if len(apollo_key.strip()) > 0:
        print(f"✅ APOLLO_API_KEY Carregada: {apollo_key[:5]}...[oculto]")
    else:
        print("❌ APOLLO_API_KEY existe mas está VAZIA (verifique se tem algo depois do =)")
else:
    print("❌ APOLLO_API_KEY: Não encontrada (Python retornou None).")
    print("   Dica: Verifique se você salvou o arquivo .env (Ctrl+O) e se o nome da variável é exato.")

print("\n------------------------------")
