from presidio_analyzer import AnalyzerEngine

# Initialize the presidio analyzer engine
analyzer = AnalyzerEngine()

def _presidio_analyzer(text: str):
    pii_result = {}
    results = analyzer.analyze(text=text, entities=["PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON"], language="en")
    found_entities = []
    if results:
        for result in results:
            found_entities.append((result.entity_type, text[result.start:result.end]))
    return found_entities


text = "hey , Rohit here, with phone number : 9198960554, PAN card: BPWCD7634D"
found_entities = _presidio_analyzer(text)
print(found_entities)