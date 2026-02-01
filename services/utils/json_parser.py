"""
JSON Parser - Robust parsing av LLM-svar som ska vara JSON.

Hanterar vanliga LLM-quirks:
- Markdown code fences (```json ... ```)
- Prefix-text ("SVAR:", "Här kommer svaret:")
- Trailing commas
- Whitespace-problem
"""

import json
import logging
import re

LOGGER = logging.getLogger('JsonParser')


def parse_llm_json(text: str, context: str = "unknown") -> dict:
    """
    Parsa JSON från LLM-svar, även om det finns text före/efter.
    
    Args:
        text: Rå LLM-respons
        context: Beskrivning för logging (t.ex. "planner_select")
    
    Returns:
        dict: Parsed JSON
    
    Raises:
        ValueError: Om JSON inte kan extraheras eller parsas
    """
    if not text or not text.strip():
        LOGGER.error(f"[{context}] Tom LLM-respons")
        raise ValueError(f"HARDFAIL: [{context}] LLM returnerade tom respons")
    
    original = text
    
    # Steg 1: Ta bort markdown code fences
    text = text.replace("```json", "").replace("```", "").strip()
    
    # Steg 2: Fixa vanliga JSON-fel (trailing commas)
    text = re.sub(r',\s*}', '}', text)
    text = re.sub(r',\s*]', ']', text)
    
    # Steg 3: Hitta STÖRSTA giltiga JSON-objekt med raw_decode
    # Prioriterar yttre objekt över inre objekt (yttre börjar tidigare i texten)
    # Detta löser problemet där inre objekt med fler nycklar väljs istället för yttre objekt
    decoder = json.JSONDecoder()
    best_object = None
    best_object_size = 0
    best_object_start = float('inf')  # Börjar så tidigt som möjligt
    best_array = None
    best_array_size = 0
    
    for i, char in enumerate(text):
        if char == '{':
            try:
                obj, end = decoder.raw_decode(text, i)
                if isinstance(obj, dict):
                    # Prioritera objekt som börjar tidigast (yttre objekt)
                    # Om två objekt börjar på samma position, ta det med flest nycklar
                    obj_size = len(obj)
                    if i < best_object_start or (i == best_object_start and obj_size > best_object_size):
                        best_object = obj
                        best_object_size = obj_size
                        best_object_start = i
                        LOGGER.debug(f"[{context}] Kandidat-objekt vid pos {i}: {obj_size} nycklar")
            except json.JSONDecodeError:
                continue
        elif char == '[':
            try:
                obj, end = decoder.raw_decode(text, i)
                if isinstance(obj, list):
                    # Ta STÖRSTA arrayen
                    arr_size = len(obj)
                    if arr_size > best_array_size:
                        best_array = obj
                        best_array_size = arr_size
            except json.JSONDecodeError:
                continue
    
    # Prioritera objekt över arrayer (LLM-svar är oftast objekt)
    if best_object is not None:
        LOGGER.debug(f"[{context}] Returnerar objekt med {best_object_size} nycklar (startade vid pos {best_object_start})")
        return best_object
    if best_array is not None:
        LOGGER.debug(f"[{context}] Returnerar array med {best_array_size} element")
        return best_array
    
    LOGGER.error(f"[{context}] Kunde inte hitta JSON-block i respons: {text[:200]}...")
    raise ValueError(f"HARDFAIL: [{context}] Inget JSON-block hittades i LLM-svar")


# --- TEST ---
if __name__ == "__main__":
    # Test 1: Ren JSON
    test1 = '{"status": "OK", "keywords": ["test"]}'
    print(f"Test 1: {parse_llm_json(test1, 'test1')}")
    
    # Test 2: JSON med markdown fences
    test2 = '```json\n{"status": "OK"}\n```'
    print(f"Test 2: {parse_llm_json(test2, 'test2')}")
    
    # Test 3: JSON med prefix-text
    test3 = 'SVAR:\n\n{"status": "OK", "data": "test"}'
    print(f"Test 3: {parse_llm_json(test3, 'test3')}")
    
    # Test 4: JSON med trailing comma
    test4 = '{"items": ["a", "b",], "count": 2,}'
    print(f"Test 4: {parse_llm_json(test4, 'test4')}")
    
    # Test 5: Lång analys före JSON
    test5 = '''Analys av frågan: Användaren söker information om möten.
    
    Baserat på dokumenten har jag identifierat följande:
    
    {"selected_ids": ["doc1", "doc2"], "reasoning": "Dessa dokument är relevanta"}'''
    print(f"Test 5: {parse_llm_json(test5, 'test5')}")
    
    # Test 6: Flera JSON-liknande strukturer - ska ta SISTA giltiga
    test6 = '''DOK 1 ([ID: abc-123]): {namn: "test"} - Kasta
DOK 2 ([ID: def-456]): Innehåller data - Kasta

Slutsats:

{"keep_ids": ["xyz-789"], "discard_ids": ["abc-123", "def-456"]}'''
    result6 = parse_llm_json(test6, 'test6')
    assert "keep_ids" in result6, "Test 6 borde hitta sista JSON med keep_ids"
    print(f"Test 6: {result6}")
    
    print("Alla tester passerade!")






