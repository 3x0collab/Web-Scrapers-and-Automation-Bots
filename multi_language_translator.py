#!/usr/bin/env python
"""
Multi-Language Translation System
Supports Armenian, Arabic, Cyrillic, Greek, and other scripts
"""
import unicodedata
import string
import threading
import re

# Translation cache and lock
_translation_cache = {}
_translation_lock = threading.Lock()

def is_non_latin(text):
    allowed_chars = set(string.punctuation + " '';")
    for char in text:
        if char in allowed_chars:
            continue
        try:
            name = unicodedata.name(char)
            if 'LATIN' not in name:
                return True
        except ValueError:
            return True
    return False

def detect_script(text):
    """Detect the primary script used in the text"""
    if not text:
        return "unknown"
    
    # Character ranges for different scripts
    armenian_chars = 'ԱԲԳԴԵԶԷԸԹԺԻԼԽԾԿՀՁՂՃՄՅՆՇՈՉՊՋՌՍՎՏՐՑՒՓՔՕՖաբգդեզէըթժիլխծկհձղճմյնշոչպջռսվտրցւփքօֆ'
    arabic_chars = 'ابتثجحخدذرزسشصضطظعغفقكلمنهوىيءآأؤإئ'
    cyrillic_chars = 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    greek_chars = 'ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθικλμνξοπρστυφχψω'
    hebrew_chars = 'אבגדהוזחטיכלמנסעפצקרשת'
    
    # Count characters for each script
    script_counts = {
        'armenian': sum(1 for char in text if char in armenian_chars),
        'arabic': sum(1 for char in text if char in arabic_chars),
        'cyrillic': sum(1 for char in text if char in cyrillic_chars),
        'greek': sum(1 for char in text if char in greek_chars),
        'hebrew': sum(1 for char in text if char in hebrew_chars),
        'latin': sum(1 for char in text if char.isalpha() and not is_non_latin(char))
    }
    
    # Return the script with the highest count
    primary_script = max(script_counts, key=script_counts.get)
    if script_counts[primary_script] > 0:
        return primary_script
    return "unknown"

def transliterate_armenian(text):
    """Armenian to Latin transliteration"""
    armenian_map = {
        'Ա': 'A', 'ա': 'a', 'Բ': 'B', 'բ': 'b', 'Գ': 'G', 'գ': 'g',
        'Դ': 'D', 'դ': 'd', 'Ե': 'E', 'ե': 'e', 'Զ': 'Z', 'զ': 'z',
        'Է': 'E', 'է': 'e', 'Ը': 'E', 'ը': 'e', 'Թ': 'T', 'թ': 't',
        'Ժ': 'Zh', 'ժ': 'zh', 'Ի': 'I', 'ի': 'i', 'Լ': 'L', 'լ': 'l',
        'Խ': 'Kh', 'խ': 'kh', 'Ծ': 'Ts', 'ծ': 'ts', 'Կ': 'K', 'կ': 'k',
        'Հ': 'H', 'հ': 'h', 'Ձ': 'Dz', 'ձ': 'dz', 'Ղ': 'Gh', 'ղ': 'gh',
        'Ճ': 'Ch', 'ճ': 'ch', 'Մ': 'M', 'մ': 'm', 'Յ': 'Y', 'յ': 'y',
        'Ն': 'N', 'ն': 'n', 'Շ': 'Sh', 'շ': 'sh', 'Ո': 'O', 'ո': 'o',
        'Չ': 'Ch', 'չ': 'ch', 'Պ': 'P', 'պ': 'p', 'Ջ': 'J', 'ջ': 'j',
        'Ռ': 'R', 'ռ': 'r', 'Ս': 'S', 'ս': 's', 'Վ': 'V', 'վ': 'v',
        'Տ': 'T', 'տ': 't', 'Ր': 'R', 'ր': 'r', 'Ց': 'Ts', 'ց': 'ts',
        'Ւ': 'U', 'ւ': 'u', 'Փ': 'P', 'փ': 'p', 'Ք': 'K', 'ք': 'k',
        'Օ': 'O', 'օ': 'o', 'Ֆ': 'F', 'ֆ': 'f'
    }
    
    result = text
    for armenian_char, latin_char in armenian_map.items():
        result = result.replace(armenian_char, latin_char)
    return result

def transliterate_arabic(text):
    """Arabic to Latin transliteration (simplified)"""
    arabic_map = {
        'ا': 'a', 'ب': 'b', 'ت': 't', 'ث': 'th', 'ج': 'j', 'ح': 'h',
        'خ': 'kh', 'د': 'd', 'ذ': 'dh', 'ر': 'r', 'ز': 'z', 'س': 's',
        'ش': 'sh', 'ص': 's', 'ض': 'd', 'ط': 't', 'ظ': 'z', 'ع': '',
        'غ': 'gh', 'ف': 'f', 'ق': 'q', 'ك': 'k', 'ل': 'l', 'م': 'm',
        'ن': 'n', 'ه': 'h', 'و': 'w', 'ى': 'a', 'ي': 'y',
        'أ': 'a', 'إ': 'i', 'آ': 'aa', 'ؤ': 'u', 'ئ': 'i'
    }
    
    result = text
    for arabic_char, latin_char in arabic_map.items():
        result = result.replace(arabic_char, latin_char)
    return result

def transliterate_cyrillic(text):
    """Cyrillic to Latin transliteration"""
    cyrillic_map = {
        'А': 'A', 'а': 'a', 'Б': 'B', 'б': 'b', 'В': 'V', 'в': 'v',
        'Г': 'G', 'г': 'g', 'Д': 'D', 'д': 'd', 'Е': 'E', 'е': 'e',
        'Ё': 'Yo', 'ё': 'yo', 'Ж': 'Zh', 'ж': 'zh', 'З': 'Z', 'з': 'z',
        'И': 'I', 'и': 'i', 'Й': 'Y', 'й': 'y', 'К': 'K', 'к': 'k',
        'Л': 'L', 'л': 'l', 'М': 'M', 'м': 'm', 'Н': 'N', 'н': 'n',
        'О': 'O', 'о': 'o', 'П': 'P', 'п': 'p', 'Р': 'R', 'р': 'r',
        'С': 'S', 'с': 's', 'Т': 'T', 'т': 't', 'У': 'U', 'у': 'u',
        'Ф': 'F', 'ф': 'f', 'Х': 'Kh', 'х': 'kh', 'Ц': 'Ts', 'ц': 'ts',
        'Ч': 'Ch', 'ч': 'ch', 'Ш': 'Sh', 'ш': 'sh', 'Щ': 'Shch', 'щ': 'shch',
        'Ъ': '', 'ъ': '', 'Ы': 'Y', 'ы': 'y', 'Ь': '', 'ь': '',
        'Э': 'E', 'э': 'e', 'Ю': 'Yu', 'ю': 'yu', 'Я': 'Ya', 'я': 'ya'
    }
    
    result = text
    for cyrillic_char, latin_char in cyrillic_map.items():
        result = result.replace(cyrillic_char, latin_char)
    return result

def transliterate_greek(text):
    """Greek to Latin transliteration"""
    greek_map = {
        'Α': 'A', 'α': 'a', 'Β': 'B', 'β': 'b', 'Γ': 'G', 'γ': 'g',
        'Δ': 'D', 'δ': 'd', 'Ε': 'E', 'ε': 'e', 'Ζ': 'Z', 'ζ': 'z',
        'Η': 'H', 'η': 'h', 'Θ': 'Th', 'θ': 'th', 'Ι': 'I', 'ι': 'i',
        'Κ': 'K', 'κ': 'k', 'Λ': 'L', 'λ': 'l', 'Μ': 'M', 'μ': 'm',
        'Ν': 'N', 'ν': 'n', 'Ξ': 'X', 'ξ': 'x', 'Ο': 'O', 'ο': 'o',
        'Π': 'P', 'π': 'p', 'Ρ': 'R', 'ρ': 'r', 'Σ': 'S', 'σ': 's', 'ς': 's',
        'Τ': 'T', 'τ': 't', 'Υ': 'Y', 'υ': 'y', 'Φ': 'F', 'φ': 'f',
        'Χ': 'Ch', 'χ': 'ch', 'Ψ': 'Ps', 'ψ': 'ps', 'Ω': 'O', 'ω': 'o'
    }
    
    result = text
    for greek_char, latin_char in greek_map.items():
        result = result.replace(greek_char, latin_char)
    return result

def transliterate_hebrew(text):
    """Hebrew to Latin transliteration"""
    hebrew_map = {
        'א': 'a', 'ב': 'b', 'ג': 'g', 'ד': 'd', 'ה': 'h', 'ו': 'v',
        'ז': 'z', 'ח': 'ch', 'ט': 't', 'י': 'y', 'כ': 'k', 'ל': 'l',
        'מ': 'm', 'נ': 'n', 'ס': 's', 'ע': '', 'פ': 'p', 'צ': 'ts',
        'ק': 'q', 'ר': 'r', 'ש': 'sh', 'ת': 't'
    }
    
    result = text
    for hebrew_char, latin_char in hebrew_map.items():
        result = result.replace(hebrew_char, latin_char)
    return result

def multi_script_transliterate(text):
    """Transliterate text based on detected script"""
    if not text:
        return text
    
    script = detect_script(text)
    original_text = text
    
    if script == 'armenian':
        result = transliterate_armenian(text)
    elif script == 'arabic':
        result = transliterate_arabic(text)
    elif script == 'cyrillic':
        result = transliterate_cyrillic(text)
    elif script == 'greek':
        result = transliterate_greek(text)
    elif script == 'hebrew':
        result = transliterate_hebrew(text)
    else:
        result = text
    
    if result != original_text:
        print(f"Transliterated {script}: '{original_text}' -> '{result}'")
        return result
    
    return text

def handle_untranslatable_text(text):
    """Handle text that couldn't be translated"""
    if not text or not isinstance(text, str):
        return text
    
    original_text = text.strip()
    
    # Try to extract Latin characters if any exist alongside non-Latin
    latin_parts = []
    non_latin_parts = []
    
    # Split by common separators and check each part
    parts = re.split(r'[;\s]+', original_text)
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        if is_non_latin(part):
            non_latin_parts.append(part)
        else:
            latin_parts.append(part)
    
    # If we have both Latin and non-Latin parts, prefer Latin
    if latin_parts:
        result = ' '.join(latin_parts)
        print(f"Extracted Latin text: '{original_text}' -> '{result}'")
        return result
    
    # If all non-Latin, try multi-script transliteration
    result = multi_script_transliterate(original_text)
    if result != original_text:
        return result
    
    # Final fallback: return original text
    print(f"No translation available for: '{original_text}'")
    return original_text

def translate_multi_language(name):
    """Multi-language translation function supporting ALL argostranslate languages"""
    if not name or not isinstance(name, str):
        return name
        
    if not is_non_latin(name):
        return name
    
    # Check cache first
    with _translation_lock:
        if name in _translation_cache:
            return _translation_cache[name]
    
    # Detect script and handle accordingly
    script = detect_script(name)
    print(f"Detected {script} script in: '{name}'")
    
    translated = name  # Default fallback
    
    # Try argostranslate for ALL available languages first
    try:
        import argostranslate.translate
        installed_languages = argostranslate.translate.get_installed_languages()
        en_lang = next((l for l in installed_languages if l.code == "en"), None)
        
        if en_lang and len(installed_languages) > 1:
            print(f"Trying argostranslate with {len(installed_languages)-1} languages...")
            
            # Try each installed language for translation
            best_translation = None
            best_score = 0
            
            for lang in installed_languages:
                if lang.code != "en":
                    try:
                        translation_obj = lang.get_translation(en_lang)
                        test_result = translation_obj.translate(name)
                        
                        # Calculate quality score for this translation
                        quality_score = calculate_translation_quality(name, test_result, lang.code)
                        
                        if quality_score > best_score and quality_score >= 0.3:  # Minimum quality threshold
                            best_translation = test_result
                            best_score = quality_score
                            print(f"Good translation from {lang.code} (score: {quality_score:.2f}): '{test_result}'")
                        else:
                            print(f"Rejected translation from {lang.code} (score: {quality_score:.2f}): '{test_result}'")
                            
                    except Exception as e:
                        print(f"Translation failed for {lang.code}: {str(e)[:50]}...")
                        continue
            
            # Use best translation if found
            if best_translation:
                translated = best_translation
                print(f"Selected best translation: '{translated}' (score: {best_score:.2f})")
            else:
                print("No good argostranslate translation found, using fallback...")
                translated = handle_untranslatable_text(name)
        else:
            print("English not found or no other languages available, using fallback...")
            translated = handle_untranslatable_text(name)
            
    except ImportError:
        print("Argostranslate not available, using transliteration fallback...")
        translated = handle_untranslatable_text(name)
    except Exception as e:
        print(f"Argostranslate error: {e}, using fallback...")
        translated = handle_untranslatable_text(name)
    
    # Cache the result
    with _translation_lock:
        _translation_cache[name] = translated
    
    return translated


def calculate_translation_quality(original, translated, lang_code):
    """Calculate quality score for a translation (0.0 to 1.0)"""
    if not translated or translated == original:
        return 0.0
    
    # Basic quality checks
    translated_clean = translated.strip()
    
    # Reject obviously bad translations
    bad_translations = [
        'no.', 'no', 'yes', 'huh', '?', 'null', 'oh, my god.', 
        'hello', 'hi', 'ok', 'okay', 'good', 'bad', 'error',
        '♪', ')', '(', '[', ']', 'page', 'map', 'picture'
    ]
    
    if translated_clean.lower() in bad_translations:
        return 0.0
    
    # Reject if contains problematic characters
    if any(char in translated for char in '♪[](){}'):
        return 0.0
    
    # Reject if too short or too long compared to original
    length_ratio = len(translated_clean) / max(len(original), 1)
    if length_ratio < 0.2 or length_ratio > 3.0:
        return 0.0
    
    # Check if translation contains original script characters (bad sign)
    script = detect_script(original)
    if script != 'latin' and script != 'unknown':
        script_chars = get_script_characters(script)
        if any(char in translated for char in script_chars):
            return 0.1  # Very low score but not zero
    
    # Start with base score
    score = 0.5
    
    # Bonus for reasonable length
    if 0.5 <= length_ratio <= 2.0:
        score += 0.2
    
    # Bonus for containing Latin characters
    latin_chars = sum(1 for char in translated if char.isalpha() and not is_non_latin(char))
    if latin_chars > 0:
        score += 0.2
    
    # Bonus for word structure (spaces between words)
    if ' ' in translated and len(translated.split()) >= 2:
        score += 0.1
    
    # Language-specific bonuses
    if lang_code in ['ar', 'fa']:  # Arabic, Persian - good for Arabic script
        if script == 'arabic':
            score += 0.2
    elif lang_code in ['ru', 'uk', 'bg']:  # Slavic languages - good for Cyrillic
        if script == 'cyrillic':
            score += 0.2
    elif lang_code in ['el']:  # Greek
        if script == 'greek':
            score += 0.2
    elif lang_code in ['he']:  # Hebrew
        if script == 'hebrew':
            score += 0.2
    
    return min(score, 1.0)


def get_script_characters(script):
    """Get character set for a script"""
    if script == 'armenian':
        return 'ԱԲԳԴԵԶԷԸԹԺԻԼԽԾԿՀՁՂՃՄՅՆՇՈՉՊՋՌՍՎՏՐՑՒՓՔՕՖաբգդեզէըթժիլխծկհձղճմյնշոչպջռսվտրցւփքօֆ'
    elif script == 'arabic':
        return 'ابتثجحخدذرزسشصضطظعغفقكلمنهوىيءآأؤإئ'
    elif script == 'cyrillic':
        return 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    elif script == 'greek':
        return 'ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθικλμνξοπρστυφχψω'
    elif script == 'hebrew':
        return 'אבגדהוזחטיכלמנסעפצקרשת'
    return ''


def list_available_languages():
    """List all available argostranslate languages"""
    try:
        import argostranslate.translate
        languages = argostranslate.translate.get_installed_languages()
        
        print(f"\nAvailable argostranslate languages ({len(languages)}):")
        print("=" * 50)
        
        for lang in sorted(languages, key=lambda x: x.code):
            print(f"  {lang.code}: {lang.name}")
        
        # Show translation pairs
        en_lang = next((l for l in languages if l.code == "en"), None)
        if en_lang:
            available_pairs = []
            for lang in languages:
                if lang.code != "en":
                    try:
                        translation = lang.get_translation(en_lang)
                        available_pairs.append(f"{lang.code}→en")
                    except:
                        pass
            
            print(f"\nTranslation pairs to English ({len(available_pairs)}):")
            print("=" * 50)
            for pair in sorted(available_pairs):
                print(f"  {pair}")
        
        return languages
        
    except ImportError:
        print("Argostranslate not available")
        return []
    except Exception as e:
        print(f"Error listing languages: {e}")
        return []

if __name__ == "__main__":
    # First, list all available languages
    print("ARGOSTRANSLATE LANGUAGE SUPPORT:")
    available_languages = list_available_languages()
    
    # Test with multiple languages
    test_names = [
        # Armenian
        'Քոչարյան Ալֆրեդ Գագիկի',
        'Kocharyan Alfred Gagik; Քոչարյան Ալֆրեդ Գագիկի',
        
        # Arabic (examples)
        'أحمد محمد علي',
        'محمود عبد الله حسن',
        
        # Cyrillic (Russian examples)
        'Иванов Петр Сергеевич',
        'Козлов Александр Владимирович',
        
        # Greek (examples)
        'Αλέξανδρος Παπαδόπουλος',
        'Γιάννης Κωνσταντίνου',
        
        # Hebrew (examples)  
        'דוד כהן לוי',
        'משה אברהם יצחק',
        
        # Chinese (if available)
        '王小明',
        '李华强',
        
        # Japanese (if available)
        '田中太郎',
        '佐藤花子',
        
        # Korean (if available)
        '김철수',
        '박영희',
        
        # Thai (if available)
        'สมชาย ใจดี',
        'สมหญิง รักษ์ดี',
        
        # Hindi (if available)
        'राम शर्मा',
        'सीता देवी',
        
        # Mixed scripts
        'John Smith; Иван Петров',
        'Ahmed Ali; أحمد علي',
        'Maria; Μαρία',
        'David; דוד'
    ]

    print('\n\nTesting Multi-Language Translation System:')
    print('=' * 60)
    
    for name in test_names:
        try:
            translated = translate_multi_language(name)
            print(f'Original:    {name}')
            print(f'Translated:  {translated}')
            print('-' * 50)
        except Exception as e:
            print(f'Error with {name}: {e}')
            print('-' * 50)
    
    # Summary of translation cache
    print(f"\nTranslation Cache Summary:")
    print(f"Cached translations: {len(_translation_cache)}")
    print("=" * 60)
