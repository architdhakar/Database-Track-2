"""
Test script to verify Groq API syntax
Testing the official Groq Python library
"""

print("Testing Groq API Syntax...\n")

print("=" * 60)
print("METHOD 1: Official groq Python library")
print("=" * 60)
try:
    from groq import Groq
    import os
    
    # Initialize client with API key
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        print("❌ GROQ_API_KEY not set in environment")
        print("   Set it with: export GROQ_API_KEY='your-key-here'")
    else:
        print("✓ API key found")
        
        client = Groq(api_key=api_key)
        
        # Test with llama-3.3-70b (latest, most capable)
        print("\n--- Testing llama-3.3-70b-versatile ---")
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "user", "content": "Say 'API Working!' in exactly 2 words"}
            ],
            temperature=0.1,
            max_tokens=50
        )
        print(f"Response: {response.choices[0].message.content}")
        
        # List available models (if API supports it)
        print("\n--- Common Groq Models ---")
        models = [
            "llama-3.3-70b-versatile",  # Latest, best quality
            "llama-3.1-8b-instant",     # Fastest
            "mixtral-8x7b-32768",       # Good balance
            "gemma2-9b-it"              # Lightweight
        ]
        for m in models:
            print(f"  - {m}")
            
except ImportError:
    print("❌ groq library not installed")
    print("   Install with: pip install groq")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("METHOD 2: Direct REST API (alternative)")
print("=" * 60)
try:
    import requests
    import os
    
    api_key = os.getenv('GROQ_API_KEY')
    if api_key:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "user", "content": "Say 'REST Working!' in exactly 2 words"}
            ],
            "temperature": 0.1,
            "max_tokens": 50
        }
        
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            result = response.json()
            answer = result['choices'][0]['message']['content']
            print(f"Response: {answer}")
        else:
            print(f"Status {response.status_code}: {response.text}")
    else:
        print("❌ GROQ_API_KEY not set")
        
except ImportError:
    print("❌ requests library not installed")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("RECOMMENDATION FOR YOUR USE CASE")
print("=" * 60)
print("""
Library: groq (official Python SDK)
Install: pip install groq

Basic Usage:
    from groq import Groq
    import os
    
    client = Groq(api_key=os.getenv('GROQ_API_KEY'))
    
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "user", "content": "Your prompt here"}
        ],
        temperature=0.1,
        max_tokens=100
    )
    
    answer = response.choices[0].message.content.strip()

Recommended Models:
- llama-3.3-70b-versatile   (Best quality, good speed)
- llama-3.1-8b-instant      (Fastest, good for simple tasks)
- mixtral-8x7b-32768        (Good balance, large context)

For YES/NO classification, use:
- Model: llama-3.1-8b-instant (fastest)
- Temperature: 0.0 (deterministic)
- Max tokens: 10 (just need "YES" or "NO")

API Key: Get from https://console.groq.com/keys
Set: export GROQ_API_KEY='your-key-here'
""")
