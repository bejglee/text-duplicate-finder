#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Párhuzamos duplikátumkereső szkript, amely a beküldött kódok legjobb
# tulajdonságait egyesíti a maximális sebesség, memóriahatékonyság és
# robusztusság érdekében.
# VERZIÓ: 2.2 (Fájlon belüli és fájlok közötti duplikátumtörlés funkcióval kibővítve)

# ==============================================================================
# HASZNÁLATI ÚTMUTATÓ (PARANCSSORI ARGUMENTUMOK)
# ==============================================================================
#
# Telepítés:
# 1. Győződj meg róla, hogy a Python 3.7 vagy újabb verziója van telepítve.
# 2. Telepítsd a szükséges csomagokat:
#    pip install -r requirements.txt vagy
#    pip install psutil xxhash tqdm
# 3. Másold a kódot egy fájlba, például `find_duplicates.py` néven.
#
# A program a parancssorból futtatható és különböző stratégiákat kínál a
# feldolgozáshoz.
#
# Példák:
#
# 1. Automatikus mód (alapértelmezett és ajánlott):
#    A program felméri a rendelkezésre álló memóriát és a fájlok méretét,
#    majd automatikusan a 'fast' vagy 'safe' módot választja.
#
#    python find_duplicates.py
#
# 2. Gyors ('fast') mód kényszerítése:
#    Akkor javasolt, ha biztosan van elég RAM a feladathoz. Ez a leggyorsabb
#    opció, de a legtöbb memóriát használja.
#
#    python find_duplicates.py --strategy fast
#
# 3. Biztonságos ('safe') mód kényszerítése:
#    Kétfázisú, memóriakímélő feldolgozás. Akkor javasolt, ha a 'fast' mód
#    túl sok memóriát használna.
#
#    python find_duplicates.py --strategy safe
#
# 4. Diszk-alapú ('disk') mód kényszerítése:
#    Extrém nagy (több tíz vagy száz GB) adatmennyiség esetén, amikor még
#    a 'safe' mód memóriaigénye is túl magas lenne. Ez a leglassabb, de
#    legbiztonságosabb opció.
#
#    python find_duplicates.py --strategy disk
#
# 5. Bemeneti mappa és egyedi elválasztó megadása:
#    python find_duplicates.py -i "C:\adatok" --hash-delimiter "|"
#
# 6. Hash-eléshez használt mezők számának módosítása:
#    python find_duplicates.py --hash-fields 4
#
# 7. Duplikátumok törlése a fájlokból (fájlon belüli és fájlok közötti):
#    python find_duplicates.py --deleteduplicates true
#
# ==============================================================================

# --- Szükséges modulok importálása ---
import os  # Operációs rendszerrel kapcsolatos műveletek (pl. CPU szám)
import gc  # Garbage Collector, memóriakezeléshez
import sys  # Rendszerspecifikus paraméterek és függvények (pl. parancssori argumentumok)
import time  # Idővel kapcsolatos függvények (pl. futási idő mérése)
import heapq  # Kupac (heap) algoritmusok, a 'disk' módhoz szükséges
import logging  # Naplózási funkciók
import argparse  # Parancssori argumentumok feldolgozása
import itertools  # Iterátorokat létrehozó függvények (pl. csoportosítás)
import shutil  # Fájlműveletek (másolás, mozgatás)
import tempfile  # Ideiglenes fájlok kezelése
from tqdm import tqdm  # Haladást jelző sáv (progress bar)
from pathlib import Path  # Objektumorientált fájlrendszer-elérési utak
from datetime import datetime  # Dátum és idő kezelése
from collections import defaultdict, Counter  # Speciális konténer típusok
from typing import Dict, Set, List, Tuple, Callable, Iterator, Any  # Típusannotációk

# Párhuzamos végrehajtáshoz szükséges modulok
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Csomagok importálása és Hashing függvény kiválasztása ---
# Megpróbáljuk importálni a 'psutil'-t a rendszererőforrások (pl. RAM) lekérdezéséhez
try:
    import psutil
    PSUTIL_AVAILABLE = True  # Jelezzük, hogy a psutil elérhető
except ImportError:
    PSUTIL_AVAILABLE = False  # Jelezzük, hogy a psutil nem elérhető

# Megpróbáljuk importálni a gyors 'xxhash' könyvtárat. Ha nem sikerül, a beépített 'hashlib'-et használjuk.
try:
    import xxhash
    HASH_ALGO_NAME = "xxhash (xxh64)"  # A használt hash algoritmus neve
    def get_hash_function() -> Callable[[bytes], str]:
        # Visszaad egy gyors hash-függvényt (xxh64)
        return lambda data: xxhash.xxh64(data, seed=2024).hexdigest()
except ImportError:
    import hashlib
    HASH_ALGO_NAME = "hashlib (blake2b)"  # A használt hash algoritmus neve
    def get_hash_function() -> Callable[[bytes], str]:
        # Visszaad egy biztonságos, de lassabb hash-függvényt (blake2b)
        return lambda data: hashlib.blake2b(data, digest_size=32).hexdigest()

# --- Konstansok ---
DEFAULT_INPUT_DIR = Path("input")  # Alapértelmezett bemeneti könyvtár
DEFAULT_OUTPUT_FILE = Path("duplicates.txt")  # Alapértelmezett kimeneti fájl
LOG_DIR = Path("logs")  # Naplófájlok könyvtára
TEMP_DIR = Path("temp_duplicate_finder")  # Ideiglenes fájlok könyvtára a 'disk' módhoz
MAX_WORKERS = max(1, (os.cpu_count() or 2) - 1)  # Párhuzamos processzek maximális száma (CPU magok - 1)
DISK_MODE_DELIMITER = "\t"  # Elválasztó karakter a 'disk' mód ideiglenes fájljaiban
# Memóriahasználat becslése a különböző stratégiákhoz
FAST_MODE_MEMORY_FACTOR = 0.4 # A 'fast' mód becsült memóriaigénye a fájlok teljes méretének 40%-a
SAFE_MODE_MEMORY_FACTOR = 0.1 # A 'safe' mód becsült memóriaigénye a fájlok teljes méretének 10%
RAM_USAGE_THRESHOLD = 0.70 # A RAM használatának maximális küszöbértéke a 'safe' és 'disk' módokhoz
DISK_CHUNK_SIZE_MB = 128  # 128 MB-os darabokban dolgozzuk fel a fájlokat disk módban
HASH_FUNCTION = get_hash_function()  # A kiválasztott hash-függvény

# --- Naplózás Beállítása ---
def setup_logger() -> Tuple[logging.Logger, logging.Logger]:
    """
    Beállítja a naplózást. Létrehoz egy loggert, amely a konzolra és egy fájlba is ír,
    valamint egy másikat, amely csak a fájlba ír.
    """
    LOG_DIR.mkdir(exist_ok=True)  # Létrehozza a log könyvtárat, ha még nem létezik
    # Egyedi logfájl név generálása az aktuális időbélyeggel
    log_filename = LOG_DIR / f"duplicate_finder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Fő logger beállítása
    logger = logging.getLogger("IdealDuplicateFinder")
    logger.setLevel(logging.INFO)  # Naplózási szint beállítása
    logger.handlers.clear()  # Esetleges korábbi handlerek törlése

    # Napló formátumának meghatározása
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Fájl handler: a log üzeneteket egy fájlba írja
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream handler: a log üzeneteket a konzolra (stdout) írja
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Csak fájlba író logger beállítása (csendesebb futáshoz)
    file_only_logger = logging.getLogger("FileOnlyLogger")
    file_only_logger.setLevel(logging.INFO)
    file_only_logger.handlers.clear()
    file_only_logger.addHandler(file_handler)

    return logger, file_only_logger

# --- Segédfüggvények ---
def normalize_line(line: str, delimiter: str, count: int) -> str:
    """
    Normalizál egy sort: eltávolítja a felesleges szóközöket,
    majd a megadott elválasztó mentén feldarabolja, és csak az első 'count' darabot fűzi össze újra.
    Ez biztosítja, hogy csak a releváns részek kerüljenek a hash-be.
    """
    return delimiter.join(line.strip().split(delimiter, count)[:count])

def hash_normalized_line(line_part: str) -> str:
    """
    A normalizált sorrészletet hash-eli a kiválasztott algoritmussal.
    A sort UTF-8 kódolású bájtokká alakítja a hash-elés előtt.
    """
    return HASH_FUNCTION(line_part.encode('utf-8'))

def get_input_files(input_dir: Path, file_pattern: str, logger: logging.Logger) -> List[Path]:
    """
    Összegyűjti a bemeneti könyvtárból a megadott mintának megfelelő fájlokat.
    A fájlokat méret szerint növekvő sorrendbe rendezi.
    """
    if not input_dir.is_dir():
        logger.error(f"A bemeneti könyvtár nem létezik: {input_dir}")
        return []

    logger.info(f"Fájlok keresése a '{input_dir}' könyvtárban a következő mintával: '{file_pattern}'")
    # A glob használatával keresi a mintának megfelelő fájlokat
    files = [f for f in input_dir.glob(file_pattern) if f.is_file()]
    # A fájlokat méret szerint rendezi, a kisebbekkel kezdve
    files.sort(key=lambda f: f.stat().st_size)

    if not files:
        logger.warning(f"Nincsenek a '{file_pattern}' mintának megfelelő feldolgozható fájlok a(z) '{input_dir}' könyvtárban.")

    return files

def write_duplicates(duplicates_data: Dict[str, List[str]], output_file: Path, logger: logging.Logger):
    """
    Kiírja a talált duplikátumokat a megadott kimeneti fájlba.
    A kulcs a duplikált sor prefixe, az érték a fájlnevek listája.
    """
    if not duplicates_data:
        logger.info("Nem található duplikált sor (sem fájlon belül, sem fájlok között).")
        with output_file.open('w', encoding='utf-8') as f:
            f.write("Nem található duplikátum.\n")
        return

    logger.info(f"{len(duplicates_data)} duplikált sor kiírása a(z) '{output_file}' fájlba...")
    # A duplikátumokat prefix szerint rendezi a konzisztens kimenetért
    sorted_prefixes = sorted(duplicates_data.keys())

    try:
        with output_file.open('w', encoding='utf-8') as f:
            for prefix in sorted_prefixes:
                f.write(f"{prefix}\n")
                # Ha csak egy fájlnév van, az fájlon belüli duplikátumot jelent
                if len(duplicates_data[prefix]) == 1:
                    f.write(f"    - (Fájlon belüli duplikátumok) {duplicates_data[prefix][0]}\n")
                else:
                    # Különben listázza az összes fájlt, ahol előfordult
                    for filename in sorted(duplicates_data[prefix]):
                        f.write(f"    - {filename}\n")
        logger.info("A duplikátumok kiírása befejeződött.")
    except IOError as e:
        logger.error(f"Hiba a kimeneti fájl írása közben: {e}")

# --- ÚJ FÁJLON BELÜLI DUPLIKÁTUMTÖRLÉSI FUNKCIÓK ---

def find_and_delete_intrafile_duplicates(file_path: Path, config: Dict[str, Any], logger: logging.Logger) -> int:
    """
    Megkeresi és törli a fájlon belüli duplikátumokat. Minden hash-hez csak az első előfordulást hagyja meg.
    Visszaadja a törölt sorok számát.
    """
    # logger.info(f"Fájlon belüli duplikátumok keresése és törlése: {file_path.name}")
    
    try:
        seen_hashes = set()
        lines_to_keep = []
        deleted_count = 0
        
        # Fájl beolvasása és duplikátumok szűrése
        with file_path.open('r', encoding='utf-8', errors='ignore') as f:
            # Fejléc megtartása
            header = next(f, None)
            if header:
                lines_to_keep.append(header)
            
            # Sorok feldolgozása
            for line in f:
                stripped_line = line.strip()
                if not stripped_line:
                    lines_to_keep.append(line)  # Üres sorok megtartása
                    continue
                
                # Hash számítása
                normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                line_hash = hash_normalized_line(normalized)
                
                # Első előfordulás megtartása, duplikátumok törlése
                if line_hash not in seen_hashes:
                    seen_hashes.add(line_hash)
                    lines_to_keep.append(line)
                else:
                    deleted_count += 1
        
        # Ha vannak törölendő sorok, fájl frissítése
        if deleted_count > 0:
            # Biztonsági mentés készítése
            backup_path = file_path.with_suffix(file_path.suffix + '.backup')
            shutil.copy2(file_path, backup_path)
            
            # Szűrt tartalom visszaírása
            with file_path.open('w', encoding='utf-8') as f:
                f.writelines(lines_to_keep)
            
            # Biztonsági mentés törlése (opcionális)
            backup_path.unlink()
            
            #logger.info(f"Fájlon belüli duplikátumtörlés: {deleted_count} sor törölve a(z) '{file_path.name}' fájlból.")
        #else:
            #logger.info(f"Nincs fájlon belüli duplikátum a(z) '{file_path.name}' fájlban.")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Hiba a fájlon belüli duplikátumtörlés során a(z) '{file_path.name}' fájlban: {e}")
        return 0

def count_intrafile_duplicates(file_path: Path, config: Dict[str, Any]) -> int:
    """
    Megszámolja egy fájlon belül a duplikált sorokat duplikátumtörlés nélkül.
    Visszaadja a duplikált sorok számát (az első előfordulás kivételével).
    """
    try:
        seen_hashes = set()
        duplicate_count = 0
        
        with file_path.open('r', encoding='utf-8', errors='ignore') as f:
            next(f, None)  # Fejléc átugrása
            for line in f:
                stripped_line = line.strip()
                if not stripped_line:
                    continue
                
                # Hash számítása
                normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                line_hash = hash_normalized_line(normalized)
                
                # Duplikátumok számlálása
                if line_hash in seen_hashes:
                    duplicate_count += 1
                else:
                    seen_hashes.add(line_hash)
        
        return duplicate_count
        
    except Exception as e:
        return 0

# --- FRISSÍTETT DUPLIKÁTUMTÖRLÉSI FUNKCIÓK ---

def collect_duplicate_lines_for_deletion(files: List[Path], duplicates_data: Dict[str, List[str]], config: Dict[str, Any], logger: logging.Logger) -> Dict[str, Dict[str, List[str]]]:
    """
    Összegyűjti a törlendő duplikált sorokat minden fájlból (csak fájlok közötti duplikátumokhoz).
    Visszaad egy szótárat: {hash: {filename: [matching_lines]}}
    """
    logger.info("--- DUPLIKÁTUMTÖRLÉS: Fájlok közötti törlendő sorok összegyűjtése ---")
    
    # Csak a fájlok közötti duplikátumokat vesszük figyelembe (több fájlban előforduló prefixek)
    inter_file_duplicate_prefixes = {prefix for prefix, file_list in duplicates_data.items() if len(file_list) > 1}
    
    if not inter_file_duplicate_prefixes:
        logger.info("Nincsenek fájlok közötti duplikátumok.")
        return {}
    
    # Hash -> teljes sor mapping létrehozása
    prefix_to_hash = {}
    hash_to_files = defaultdict(set)
    
    # Minden fájl átnézése a duplikált sorok megkeresésére
    for file_path in tqdm(files, desc="Fájlok közötti duplikált sorok keresése"):
        try:
            with file_path.open('r', encoding='utf-8', errors='ignore') as f:
                header = next(f, None)  # Fejléc mentése
                for line in f:
                    stripped_line = line.strip()
                    if not stripped_line:
                        continue
                    
                    # Prefix létrehozása
                    prefix = stripped_line[:config['write_length']]
                    
                    # Ha ez a prefix fájlok közötti duplikátum
                    if prefix in inter_file_duplicate_prefixes:
                        # Hash számítása
                        normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                        line_hash = hash_normalized_line(normalized)
                        
                        # Hash -> prefix mapping
                        if line_hash not in prefix_to_hash:
                            prefix_to_hash[line_hash] = prefix
                        
                        # Hash -> fájlok mapping
                        hash_to_files[line_hash].add(file_path.name)
                        
        except Exception as e:
            logger.error(f"Hiba a fájlok közötti duplikált sorok keresése közben a(z) '{file_path.name}' fájlban: {e}")
    
    # Csak azok a hash-ek érdekesek, amelyek több fájlban is előfordulnak
    multi_file_duplicates = {h: files for h, files in hash_to_files.items() if len(files) > 1}
    
    logger.info(f"Összesen {len(multi_file_duplicates)} hash található több fájlban is.")
    
    # Most összegyűjtjük a tényleges sorokat minden hash-hez
    lines_to_delete = defaultdict(lambda: defaultdict(list))
    
    for file_path in tqdm(files, desc="Fájlok közötti törlendő sorok részletes összegyűjtése"):
        try:
            with file_path.open('r', encoding='utf-8', errors='ignore') as f:
                header = next(f, None)
                for line in f:
                    stripped_line = line.strip()
                    if not stripped_line:
                        continue
                    
                    # Hash számítása
                    normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                    line_hash = hash_normalized_line(normalized)
                    
                    # Ha ez a hash multi-file duplikátum
                    if line_hash in multi_file_duplicates:
                        lines_to_delete[line_hash][file_path.name].append(stripped_line)
                        
        except Exception as e:
            logger.error(f"Hiba a fájlok közötti törlendő sorok részletes összegyűjtése közben a(z) '{file_path.name}' fájlban: {e}")
    
    return lines_to_delete

def delete_duplicate_rows(files: List[Path], lines_to_delete: Dict[str, Dict[str, List[str]]], duplicates_data: Dict[str, List[str]], config: Dict[str, Any], logger: logging.Logger) -> Dict[str, List[str]]:
    """
    Törli a duplikált sorokat a fájlokból. Minden hash esetében az első fájlban hagyja meg az első előfordulást,
    a többi fájlból törli az összes előfordulást.
    Ezután kezeli a fájlon belüli duplikátumokat is.
    """
    logger.info("--- DUPLIKÁTUMTÖRLÉS: Duplikált sorok törlése a fájlokból ---")
    
    modified_duplicates_data = duplicates_data.copy()
    inter_file_deleted_counts = defaultdict(int)
    intra_file_deleted_counts = defaultdict(int)
    
    # 1. FÁJLOK KÖZÖTTI DUPLIKÁTUMOK TÖRLÉSE
    logger.info("1. Fájlok közötti duplikátumok törlése...")
    
    # Minden hash esetében meghatározzuk, hogy melyik fájlban hagyjuk meg az első előfordulást
    for line_hash, file_lines_map in lines_to_delete.items():
        if not file_lines_map:
            continue
            
        # Fájlok sorba rendezése (első fájl = megmarad, többi = törlés)
        sorted_files = sorted(file_lines_map.keys())
        keep_file = sorted_files[0]  # Az első fájlban hagyjuk meg
        delete_files = sorted_files[1:]  # A többi fájlból töröljük
        
        logger.info(f"Hash {line_hash[:8]}...: megtartás -> {keep_file}, törlés -> {delete_files}")
        
        # Törlés végrehajtása
        for delete_file in delete_files:
            file_path = None
            for f in files:
                if f.name == delete_file:
                    file_path = f
                    break
            
            if not file_path:
                logger.warning(f"Nem található fájl: {delete_file}")
                continue
                
            lines_to_remove = file_lines_map[delete_file]
            if delete_rows_from_file(file_path, lines_to_remove, logger):
                inter_file_deleted_counts[delete_file] += len(lines_to_remove)
    
    # 2. FÁJLON BELÜLI DUPLIKÁTUMOK TÖRLÉSE
    logger.info("2. Fájlon belüli duplikátumok törlése...")
    
    for file_path in tqdm(files, desc="Fájlon belüli duplikátumtörlés"):
        intra_deleted = find_and_delete_intrafile_duplicates(file_path, config, logger)
        if intra_deleted > 0:
            intra_file_deleted_counts[file_path.name] = intra_deleted
    
    # 3. KIMENETI ADATOK FRISSÍTÉSE
    # Fájlok közötti duplikátumok frissítése
    for prefix, file_list in list(modified_duplicates_data.items()):
        if len(file_list) > 1:  # Fájlok közötti duplikátum
            updated_file_list = []
            for filename in file_list:
                if inter_file_deleted_counts[filename] > 0:
                    updated_file_list.append(f"{filename} -> Törölve {inter_file_deleted_counts[filename]} sor")
                else:
                    updated_file_list.append(filename)
            modified_duplicates_data[prefix] = updated_file_list
        else:  # Fájlon belüli duplikátum - frissítjük a meglévő bejegyzést
            filename = file_list[0]
            if intra_file_deleted_counts[filename] > 0:
                modified_duplicates_data[prefix] = [f"{filename} -> Törölve {intra_file_deleted_counts[filename]} sor"]
    
    # 4. ÚJ FÁJLON BELÜLI DUPLIKÁTUMOK HOZZÁADÁSA (csak azok, amelyek még nem szerepelnek)
    # Azok a fájlok, amelyeknek van fájlon belüli duplikátuma, de még nem szerepelnek a kimenetben
    for filename, deleted_count in intra_file_deleted_counts.items():
        if deleted_count > 0:
            # Ellenőrizzük, hogy ez a fájl már szerepel-e valamelyik duplikátumban
            already_listed = False
            for file_list in modified_duplicates_data.values():
                for listed_file in file_list:
                    # Eltávolítjuk a " -> Törölve X sor" részt az összehasonlításhoz
                    clean_filename = listed_file.split(' ->')[0]
                    if clean_filename == filename:
                        already_listed = True
                        break
                if already_listed:
                    break
            
            # Ha még nem szerepel, akkor hozzáadjuk
            if not already_listed:
                # Dummy prefix a fájlon belüli duplikátumokhoz
                dummy_prefix = f"(Csak fájlon belüli duplikátumok - {filename})"
                modified_duplicates_data[dummy_prefix] = [f"{filename} -> Törölve {deleted_count} sor"]
    
    # Összegzés naplózása
    total_inter_deleted = sum(inter_file_deleted_counts.values())
    total_intra_deleted = sum(intra_file_deleted_counts.values())
    total_deleted = total_inter_deleted + total_intra_deleted
    
    logger.info(f"Duplikátumtörlés befejezve.")
    logger.info(f"Fájlok közötti duplikátumok: {total_inter_deleted} sor törölve {len(inter_file_deleted_counts)} fájlból.")
    logger.info(f"Fájlon belüli duplikátumok: {total_intra_deleted} sor törölve {len(intra_file_deleted_counts)} fájlból.")
    logger.info(f"Összesen: {total_deleted} sor törölve.")
    
    return modified_duplicates_data

def delete_rows_from_file(file_path: Path, lines_to_remove: List[str], logger: logging.Logger) -> bool:
    """
    Biztonságosan törli a megadott sorokat egy fájlból.
    Ideiglenes fájlt használ a biztonságos módosításhoz.
    """
    try:
        # Set létrehozása a gyors kereséshez
        lines_to_remove_set = set(lines_to_remove)
        
        # Ideiglenes fájl létrehozása
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.tmp') as temp_file:
            temp_path = Path(temp_file.name)
            
            # Eredeti fájl olvasása és szűrt tartalom írása
            with file_path.open('r', encoding='utf-8', errors='ignore') as original_file:
                # Fejléc megtartása
                header = next(original_file, None)
                if header:
                    temp_file.write(header)
                
                # Sorok szűrése
                for line in original_file:
                    stripped_line = line.strip()
                    if stripped_line not in lines_to_remove_set:
                        temp_file.write(line)
        
        # Eredeti fájl biztonsági mentése
        backup_path = file_path.with_suffix(file_path.suffix + '.backup')
        shutil.copy2(file_path, backup_path)
        
        # Ideiglenes fájl áthelyezése az eredeti helyre
        shutil.move(str(temp_path), str(file_path))
        
        # Biztonsági mentés törlése (opcionális, megjegyzésbe tehető)
        backup_path.unlink()
        
        logger.info(f"Sikeresen törölve {len(lines_to_remove)} sor a(z) '{file_path.name}' fájlból.")
        return True
        
    except Exception as e:
        logger.error(f"Hiba a sorok törlése közben a(z) '{file_path.name}' fájlban: {e}")
        # Takarítás hiba esetén
        if 'temp_path' in locals() and temp_path.exists():
            temp_path.unlink()
        return False

# --- Worker Függvények ---

def process_file_fast(file_path: Path, file_id: int, config: Dict[str, Any]) -> Dict[str, Tuple[str, int]]:
    """
    'fast' stratégia worker függvénye. Egyetlen fájlt dolgoz fel.
    Visszaad egy szótárat, ahol a kulcs a hash, az érték pedig egy (prefix, darabszám) tuple.
    """
    local_hashes = {}  # Az adott fájlon belüli hash-eket tárolja
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)  # Fejléc átugrása
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue  # Üres sorok kihagyása
            
            # Sor normalizálása és hash-elése
            normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
            h = hash_normalized_line(normalized)
            
            # Ha a hash új, eltároljuk a prefix-szel és 1-es darabszámmal
            if h not in local_hashes:
                local_hashes[h] = [stripped_line[:config['write_length']], 1]
            else:
                # Ha már létezik, növeljük a darabszámot
                local_hashes[h][1] += 1
    
    # A végső szótár összeállítása a feldolgozott adatokból
    return {h: (data[0], data[1]) for h, data in local_hashes.items()}

def process_file_safe_pass1(file_path: Path, config: Dict[str, Any]) -> Counter:
    """
    'safe' stratégia első fázisának worker függvénye.
    Csak a hash-eket és azok előfordulási számát gyűjti össze egy fájlban.
    """
    local_hashes = Counter()  # Counter objektum a hatékony számláláshoz
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)  # Fejléc átugrása
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                local_hashes[hash_normalized_line(normalized)] += 1
    return local_hashes

def process_file_safe_pass2(file_path: Path, duplicate_hashes: Set[str], config: Dict[str, Any]) -> Dict[str, str]:
    """
    'safe' stratégia második fázisának worker függvénye.
    Csak a duplikáltnak talált hash-ekhez tartozó sor-prefixeket gyűjti ki.
    """
    results = {}
    if not duplicate_hashes: return results  # Ha nincsenek duplikátumok, nincs teendő
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)  # Fejléc átugrása
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue
            normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
            h = hash_normalized_line(normalized)
            # Csak akkor dolgozzuk fel, ha a hash a duplikáltak között van
            if h in duplicate_hashes and h not in results:
                results[h] = stripped_line[:config['write_length']]
    return results

def process_and_sort_chunk_disk(file_info: Tuple[Path, int, Path, Dict[str, Any]]) -> List[Path]:
    """
    'disk' stratégia worker függvénye. Egy fájlt darabokban (chunk) olvas be,
    feldolgozza, rendezi a darabokat hash szerint, és ideiglenes fájlokba írja őket.
    """
    file_path, file_id, temp_dir, config = file_info
    chunk_files = []  # Az ehhez a fájlhoz tartozó ideiglenes chunk fájlok listája
    chunk_size_bytes = DISK_CHUNK_SIZE_MB * 1024 * 1024
    
    try:
        with file_path.open('r', encoding='utf-8', errors='ignore') as f_in:
            next(f_in, None)  # Fejléc átugrása
            chunk_idx = 0
            while True:
                lines = f_in.readlines(chunk_size_bytes)  # Beolvas egy darabot
                if not lines:
                    break  # Fájl vége
                
                processed_lines = []
                for line in lines:
                    stripped_line = line.strip()
                    if not stripped_line: continue
                    
                    prefix = stripped_line[:config['write_length']].replace(DISK_MODE_DELIMITER, " ")
                    normalized = normalize_line(stripped_line, config['hash_delimiter'], config['hash_fields'])
                    if not normalized: continue
                    
                    h = hash_normalized_line(normalized)
                    processed_lines.append((h, file_id, prefix))
                
                # A darabon belüli sorok rendezése hash szerint
                processed_lines.sort(key=lambda x: x[0])
                
                # A rendezett darab kiírása egy ideiglenes fájlba
                temp_chunk_path = temp_dir / f"hashes_{file_id}_chunk_{chunk_idx}.tmp"
                with temp_chunk_path.open('w', encoding='utf-8') as f_out:
                    f_out.writelines(f"{h}{DISK_MODE_DELIMITER}{fid}{DISK_MODE_DELIMITER}{pref}\n" for h, fid, pref in processed_lines)
                
                chunk_files.append(temp_chunk_path)
                chunk_idx += 1
    except Exception as e:
        # Hiba naplózása, de a már létrehozott chunk fájlokkal visszatérünk
        logging.error(f"Error processing chunk for {file_path.name}: {e}")

    return chunk_files

# --- FRISSÍTETT Stratégia Vezérlő Függvények ---

def run_strategy_fast(files: List[Path], id_to_file_map: Dict[int, str], config: Dict[str, Any], logger: logging.Logger, file_only_logger: logging.Logger):
    """
    'fast' stratégia végrehajtása.
    Minden fájlt párhuzamosan feldolgoz, és az eredményeket a memóriában egyesíti.
    """
    logger.info("--- Indítás: FAST (memóriaigényes) stratégia ---")
    # Globális szótár a hash-ek, prefixek és fájl-számlálók tárolására
    global_hashes = defaultdict(lambda: ("", Counter()))

    # ProcessPoolExecutor a párhuzamos végrehajtáshoz
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Feladatok beküldése a workereknek
        future_to_id = {executor.submit(process_file_fast, path, fid, config): fid for fid, path in enumerate(files)}
        # Eredmények begyűjtése, amint elkészülnek (progress bar-ral)
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="FAST feldolgozás"):
            file_id = future_to_id[future]
            try:
                partial_results = future.result()  # Worker eredményének lekérése
                # Részeredmények egyesítése a globális szótárba
                for h, (prefix, count) in partial_results.items():
                    if not global_hashes[h][0]:
                        global_hashes[h] = (prefix, global_hashes[h][1])
                    global_hashes[h][1][file_id] = count
                file_only_logger.info(f"Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba a(z) '{id_to_file_map[file_id]}' feldolgozása közben: {e}", exc_info=True)

    # Kimeneti adatok előkészítése a duplikátumokból
    output_data = {}
    intra_file_duplicates = {}
    
    for _, (prefix, file_counts) in global_hashes.items():
        total_occurrences = sum(file_counts.values())
        
        # Fájlok közötti duplikátumok (több fájlban előfordul)
        if len(file_counts) > 1:
            file_ids = file_counts.keys()
            output_data[prefix] = [id_to_file_map[fid] for fid in sorted(list(file_ids))]
        # Fájlon belüli duplikátumok (egy fájlban, de többször)
        elif total_occurrences > 1:
            file_id = list(file_counts.keys())[0]
            filename = id_to_file_map[file_id]
            intra_file_duplicates[prefix] = [filename]

    # Fájlon belüli duplikátumok hozzáadása a kimenethez
    output_data.update(intra_file_duplicates)

    # Duplikátumtörlés végrehajtása, ha engedélyezve van
    if config.get('deleteduplicates', False):
        lines_to_delete = collect_duplicate_lines_for_deletion(files, output_data, config, logger)
        output_data = delete_duplicate_rows(files, lines_to_delete, output_data, config, logger)

    # Duplikátumok kiírása fájlba
    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

def run_strategy_safe(files: List[Path], id_to_file_map: Dict[int, str], config: Dict[str, Any], logger: logging.Logger, file_only_logger: logging.Logger):
    """
    'safe' stratégia végrehajtása. Két fázisban dolgozik a memóriaterhelés csökkentése érdekében.
    1. Fázis: Csak a hash-eket és azok darabszámát gyűjti.
    2. Fázis: Csak a duplikált hash-ekhez tartozó sor-prefixeket gyűjti be.
    """
    logger.info("--- Indítás: SAFE (memóriakímélő) stratégia ---")
    
    # --- 1. FÁZIS: Hash-ek és előfordulásaik gyűjtése ---
    logger.info("--- 1. FÁZIS: Hash-ek és előfordulásaik gyűjtése ---")
    hash_to_file_counts = defaultdict(Counter)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_file_safe_pass1, path, config): fid for fid, path in enumerate(files)}
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="SAFE 1. fázis"):
            file_id = future_to_id[future]
            try:
                hashes_with_counts = future.result()
                for h, count in hashes_with_counts.items():
                    hash_to_file_counts[h][file_id] = count
                file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba (1. fázis) a(z) '{id_to_file_map[file_id]}' feldolgozása során: {e}", exc_info=True)

    # Azon hash-ek kiválasztása, amelyek duplikátumok (fájlok között vagy fájlon belül)
    duplicate_hashes = set()
    for h, fc in hash_to_file_counts.items():
        total_count = sum(fc.values())
        file_count = len(fc)
        # Duplikátum, ha több fájlban előfordul VAGY egy fájlban többször
        if file_count > 1 or total_count > 1:
            duplicate_hashes.add(h)
    
    if not duplicate_hashes:
        write_duplicates({}, DEFAULT_OUTPUT_FILE, logger)
        return

    # --- 2. FÁZIS: Duplikált sorok adatainak gyűjtése ---
    logger.info(f"Összesen {len(duplicate_hashes)} egyedi duplikált hash azonosítva.")
    logger.info("--- 2. FÁZIS: Duplikált sorok adatainak gyűjtése ---")
    hash_to_prefix_map = {}
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # A második fázisban csak a duplikált hash-ek listáját adjuk át a workereknek
        future_to_file = {executor.submit(process_file_safe_pass2, path, duplicate_hashes, config): path for path in files}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="SAFE 2. fázis"):
            try:
                hash_to_prefix_map.update(future.result())
            except Exception as e:
                logger.error(f"Hiba (2. fázis) a(z) '{future_to_file[future].name}' feldolgozása során: {e}", exc_info=True)
    
    # Kimeneti adatok összeállítása a két fázis eredményeiből
    output_data = {}
    for h, prefix in hash_to_prefix_map.items():
        if h in duplicate_hashes:
            file_counts = hash_to_file_counts[h]
            total_count = sum(file_counts.values())
            file_count = len(file_counts)
            
            # Fájlok közötti duplikátum
            if file_count > 1:
                file_ids = file_counts.keys()
                output_data[prefix] = [id_to_file_map[fid] for fid in sorted(list(file_ids))]
            # Fájlon belüli duplikátum
            elif total_count > 1:
                file_id = list(file_counts.keys())[0]
                output_data[prefix] = [id_to_file_map[file_id]]

    # Duplikátumtörlés végrehajtása, ha engedélyezve van
    if config.get('deleteduplicates', False):
        lines_to_delete = collect_duplicate_lines_for_deletion(files, output_data, config, logger)
        output_data = delete_duplicate_rows(files, lines_to_delete, output_data, config, logger)

    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

def run_strategy_disk(files: List[Path], id_to_file_map: Dict[int, str], config: Dict[str, Any], logger: logging.Logger, file_only_logger: logging.Logger):
    """
    'disk' stratégia végrehajtása. Nagyon nagy adatmennyiséghez, külső rendezést (external sort) használ.
    1. Fázis: Minden fájlt darabokban feldolgoz, és a hash-eket rendezve ideiglenes fájlokba írja.
    2. Fázis: Lépcsőzetes összefésülés (cascading merge) a "túl sok nyitott fájl" hiba elkerülésére.
    3. Fázis: A végső, rendezett fájl feldolgozása a duplikátumok azonosítására.
    """
    logger.info("--- Indítás: DISK (optimalizált diszk-alapú) stratégia ---")
    if TEMP_DIR.exists():
        for f in TEMP_DIR.iterdir(): f.unlink()
    TEMP_DIR.mkdir(exist_ok=True)

    all_temp_files = []
    try:
        # --- 1. FÁZIS: Adatok feldolgozása és rendezett ideiglenes fájlokba írása ---
        logger.info("--- 1. FÁZIS: Adatok feldolgozása és rendezett ideiglenes fájlokba írása (darabolva) ---")
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [(path, fid, TEMP_DIR, config) for fid, path in enumerate(files)]
            future_to_task = {executor.submit(process_and_sort_chunk_disk, task): task for task in tasks}
            
            for future in tqdm(as_completed(future_to_task), total=len(future_to_task), desc="DISK 1. fázis"):
                task_path, task_id = future_to_task[future][0], future_to_task[future][1]
                try:
                    chunk_files = future.result()
                    all_temp_files.extend(chunk_files)
                    file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[task_id]}")
                except Exception as e:
                    logger.error(f"Hiba a(z) '{task_path.name}' (ID: {task_id}) feldolgozása során: {e}", exc_info=True)

        # --- 2. FÁZIS: Lépcsőzetes összefésülés (Cascading Merge) ---
        logger.info("--- 2. FÁZIS: Lépcsőzetes összefésülés (Cascading Merge) ---")
        
        def merge_files(files_to_merge: List[Path], output_path: Path):
            """Segédfüggvény, amely összefésül egy listányi fájlt egyetlen kimeneti fájlba."""
            open_files_list = []
            try:
                for f in files_to_merge:
                    open_files_list.append(f.open('r', encoding='utf-8'))
                
                with output_path.open('w', encoding='utf-8') as f_out:
                    merged_lines = heapq.merge(*open_files_list)
                    f_out.writelines(merged_lines)
            finally:
                for f in open_files_list:
                    f.close()

        merge_level = 0
        temp_files_for_merge = all_temp_files.copy() # Másolatot használunk, hogy az eredeti lista megmaradjon a takarításhoz
        while len(temp_files_for_merge) > 1:
            merge_level += 1
            logger.info(f"Összefésülési szint {merge_level}, {len(temp_files_for_merge)} fájl feldolgozása...")
            merged_level_files = []
            
            for i in tqdm(range(0, len(temp_files_for_merge), config['merge_batch_size']), desc=f"Összefésülés szint {merge_level}"):
                batch = temp_files_for_merge[i:i + config['merge_batch_size']]
                if not batch: continue
                
                output_path = TEMP_DIR / f"merged_{merge_level}_{i}.tmp"
                merge_files(batch, output_path)
                merged_level_files.append(output_path)
                all_temp_files.append(output_path) # Hozzáadjuk a takarítandó fájlok listájához

            temp_files_for_merge = merged_level_files

        final_merged_file = temp_files_for_merge[0] if temp_files_for_merge else None

        # --- 3. FÁZIS: Duplikátumok keresése a végső összefésült fájlban ---
        logger.info("--- 3. FÁZIS: Duplikátumok keresése a végső összefésült fájlban ---")
        output_data = {}
        if final_merged_file and final_merged_file.exists():
            with final_merged_file.open('r', encoding='utf-8') as f:
                line_grouper = itertools.groupby(f, key=lambda line: line.split(DISK_MODE_DELIMITER, 1)[0])
                for h, group in tqdm(line_grouper, desc="DISK 3. fázis - Duplikátumkeresés"):
                    group_items = list(group)
                    if len(group_items) > 1:
                        file_ids, prefix = set(), ""
                        total_occurrences = len(group_items)
                        
                        for item in group_items:
                            try:
                                _, fid_str, current_prefix = item.strip().split(DISK_MODE_DELIMITER, 2)
                                file_ids.add(int(fid_str))
                                if not prefix: prefix = current_prefix
                            except (ValueError, IndexError): continue
                        
                        if prefix:
                            file_names = [id_to_file_map[fid] for fid in sorted(list(file_ids))]
                            output_data[prefix] = file_names
        
        # Duplikátumtörlés végrehajtása, ha engedélyezve van
        if config.get('deleteduplicates', False):
            lines_to_delete = collect_duplicate_lines_for_deletion(files, output_data, config, logger)
            output_data = delete_duplicate_rows(files, lines_to_delete, output_data, config, logger)

        write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

    finally:
        logger.info("Ideiglenes fájlok törlése...")
        try:
            # A finally blokkban a all_temp_files listában szereplő összes fájlt töröljük
            for f in all_temp_files:
                if f.exists():
                    f.unlink()
            if TEMP_DIR.exists():
                TEMP_DIR.rmdir()
        except Exception as e:
            logger.warning(f"Nem sikerült minden ideiglenes fájlt törölni: {e}")

# --- Fő Vezérlés és Stratégiaválasztás ---
def estimate_average_line_length(file: Path, logger: logging.Logger, max_lines: int = 10000) -> float:
    """
    Megbecsüli egy fájl átlagos sorhosszát egy minta alapján.
    Ez segít a 'disk' mód tárhelyigényének pontosabb becslésében.
    """
    total_length = 0
    line_count = 0
    try:
        with file.open('r', encoding='utf-8', errors='ignore') as f:
            next(f)  # Fejléc kihagyása
            for line in f:
                stripped = line.strip()
                if stripped:
                    total_length += len(stripped)
                    line_count += 1
                    if line_count >= max_lines:  # Elég a minta
                        break
        if line_count == 0:
            logger.warning(f"Nem sikerült érvényes sort találni a(z) '{file.name}' fájlban. Alapértelmezett érték lesz használva.")
            return 150.0  # Visszaad egy ésszerű alapértelmezett értéket
        return total_length / line_count
    except Exception as e:
        logger.warning(f"Hiba az átlagos sorhossz becslése közben: {e}. Alapértelmezett érték: 150.0")
        return 150.0

def auto_select_strategy(files: List[Path], wlength: int, logger: logging.Logger) -> str:
    """
    Automatikusan kiválasztja a legmegfelelőbb stratégiát a rendelkezésre álló memória
    és a feldolgozandó adatok mérete alapján.
    """
    if not PSUTIL_AVAILABLE:
        logger.warning("A 'psutil' csomag nem található. A 'pip install psutil' parancs futtatása javasolt.")
        logger.warning("Biztonsági okokból a 'safe' stratégia lesz használva.")
        return "safe"
    try:
        total_size_bytes = sum(f.stat().st_size for f in files)
        available_ram_bytes = psutil.virtual_memory().available
        ram_limit = available_ram_bytes * RAM_USAGE_THRESHOLD
        
        # Memóriaigény becslése a különböző módokhoz
        est_fast_mode_ram = total_size_bytes * FAST_MODE_MEMORY_FACTOR
        est_safe_mode_ram = total_size_bytes * SAFE_MODE_MEMORY_FACTOR

        # Legnagyobb fájl kiválasztása és átlagos sorhossz becslése a pontosabb 'disk' mód becsléshez
        largest_file = max(files, key=lambda f: f.stat().st_size)
        avg_line_length = estimate_average_line_length(largest_file, logger)

        # 'disk' mód várható tárhelyigényének becslése
        disk_record_length = 16 + 2 + 3 + wlength + 1  # hash + tabok + file_id + prefix + newline
        est_disk_space_bytes = int(total_size_bytes * (disk_record_length / avg_line_length))

        logger.info("Automatikus stratégiaválasztás:")
        logger.info(f"  - Fájlok teljes mérete: {total_size_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Rendelkezésre álló RAM: {available_ram_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Memóriaküszöb ({RAM_USAGE_THRESHOLD*100:.0f}%): {ram_limit / (1024**3):.3f} GB")
        logger.info(f"  - 'fast' mód becsült memóriaigénye: {est_fast_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'safe' mód becsült memóriaigénye: {est_safe_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'disk' mód becsült tárhelyigénye: {est_disk_space_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Átlagos sorhossz becslés a '{largest_file.name}' fájlból: {avg_line_length:.1f} karakter")

        # Döntési logika
        if est_fast_mode_ram < ram_limit:
            logger.info("  -> Döntés: Elegendő a memória. A 'FAST' stratégia kiválasztva.")
            return "fast"
        elif est_safe_mode_ram < ram_limit:
            logger.info("  -> Döntés: A 'FAST' túl memóriaigényes, de a 'SAFE' belefér. A 'SAFE' stratégia kiválasztva.")
            return "safe"
        else:
            logger.info("  -> Döntés: Még a 'SAFE' mód is túl sok memóriát igényel. A 'DISK' stratégia kiválasztva.")
            return "disk"
    except Exception as e:
        logger.warning(f"Nem sikerült az automatikus stratégiaválasztás ({e}). Alapértelmezett: 'safe' mód.")
        return "safe"

def str_to_bool(value: str) -> bool:
    """
    Konvertál egy string értéket boolean-ná.
    Elfogadott értékek: 'true', 'false' (kis/nagy betű érzéketlen).
    """
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError(f"Boolean érték várható ('true' vagy 'false'), de '{value}' érkezett.")

def main():
    """
    A program fő belépési pontja.
    Feldolgozza a parancssori argumentumokat, beállítja a környezetet,
    kiválasztja és futtatja a megfelelő stratégiát.
    """
    # Parancssori argumentumok feldolgozójának létrehozása
    parser = argparse.ArgumentParser(
        description="Párhuzamos duplikátumkereső nagy szöveges fájlokban (Verzió 2.2 - Fájlon belüli és fájlok közötti duplikátumtörlés funkcióval).",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Argumentumok hozzáadása
    parser.add_argument(
        '-i', '--input', type=Path, default=DEFAULT_INPUT_DIR,
        help=f"Bemeneti könyvtár (alapértelmezett: '{DEFAULT_INPUT_DIR}')"
    )
    parser.add_argument(
        '-s', '--strategy', choices=['auto', 'fast', 'safe', 'disk'], default='auto',
        help="Feldolgozási stratégia (auto, fast, safe, disk)."
    )
    parser.add_argument(
        '-wl', '--write-length', type=int, default=47,
        help="A duplikátumként kiírt sorok prefixének hossza (alapértelmezett: 47)."
    )
    parser.add_argument(
        '-hf', '--hash-fields', type=int, default=6,
        help="A hash-eléshez felhasznált mezők száma (alapértelmezett: 6)."
    )
    parser.add_argument(
        '-hd', '--hash-delimiter', type=str, default=';',
        help="A mezőket elválasztó karakter (alapértelmezett: ';')."
    )
    parser.add_argument(
        '-fp', '--file-pattern', type=str, default='*.csv',
        help="Fájl minta a bemeneti fájlok szűréséhez (pl. '*_2024_*.csv')."
    )
    parser.add_argument(
        '-mbs', '--merge-batch-size', type=int, default=256,
        help="Hány ideiglenes fájlt fésüljön össze egyszerre a 'disk' módban (alapértelmezett: 256)."
    )
    # ÚJ ARGUMENTUM: Duplikátumtörlés engedélyezése
    parser.add_argument(
        '-dd', '--deleteduplicates', type=str_to_bool, default=False,
        help="Duplikált sorok törlése a fájlokból (fájlon belüli és fájlok közötti) (true/false, alapértelmezett: false)."
    )
    args = parser.parse_args()  # Argumentumok beolvasása
    config = vars(args)  # Argumentumok szótárrá alakítása a könnyebb átadhatóságért

    # Naplózás beállítása
    logger, file_only_logger = setup_logger()

    start_time = time.time()  # Futási idő mérésének indítása
    logger.info(f"Program indítása {MAX_WORKERS} worker processzel.")
    logger.info(f"Használt hash algoritmus: {HASH_ALGO_NAME}")
    logger.info(f"Konfiguráció: {config}")
    
    # Duplikátumtörlés figyelmeztetés
    if config.get('deleteduplicates', False):
        logger.warning("FIGYELEM: A duplikátumtörlés funkció be van kapcsolva!")
        logger.warning("A duplikált sorok törlésre kerülnek a fájlokból!")
        logger.warning("Ez magában foglalja a fájlon belüli és a fájlok közötti duplikátumokat is!")
        logger.warning("Biztonsági mentés készítése ajánlott a feldolgozás előtt!")

    # Bemeneti fájlok begyűjtése
    files = get_input_files(args.input, args.file_pattern, logger)
    if not files:
        return  # Ha nincsenek fájlok, a program leáll

    # Fájlnevek és egyedi azonosítók összerendelése
    id_to_file_map = {i: f.name for i, f in enumerate(files)}

    # Stratégia kiválasztása
    strategy = args.strategy
    if strategy == 'auto':
        strategy = auto_select_strategy(files, args.write_length, logger)

    # A kiválasztott stratégiához tartozó függvény meghatározása
    strategy_map = {
        'fast': run_strategy_fast,
        'safe': run_strategy_safe,
        'disk': run_strategy_disk,
    }

    # A megfelelő stratégia futtatása
    strategy_map[strategy](files, id_to_file_map, config, logger, file_only_logger)

    # gc.collect() # Opcionális szemétgyűjtés a végén
    end_time = time.time()  # Futási idő mérésének leállítása
    logger.info(f"A futás befejeződött. Teljes idő: {end_time - start_time:.2f} másodperc.")

# A szkript belépési pontja, ha közvetlenül futtatják
if __name__ == '__main__':
    # Windows-specifikus beállítás a helyes karakterkódolásért a konzolon
    if sys.platform.startswith('win'):
        try:
            os.system('chcp 65001 > nul')  # UTF-8 kódlap beállítása
        except Exception:
            pass
    main()  # A fő függvény meghívása