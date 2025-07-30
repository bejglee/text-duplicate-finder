#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Párhuzamos duplikátumkereső szkript, amely a beküldött kódok legjobb
# tulajdonságait egyesíti a maximális sebesség, memóriahatékonyság és
# robusztusság érdekében.

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
# 5. Bemeneti mappa megadása:
#    Az '--input' vagy '-i' kapcsolóval adható meg a bemeneti mappa.
#
#    python find_duplicates.py --input "C:\adatok\input_mappa"
#
# ==============================================================================

import os
import gc
import sys
import time
import logging
import argparse
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, Set, List, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Csomagok importálása és Hashing függvény kiválasztása ---
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import xxhash
    HASH_ALGO_NAME = "xxhash (xxh64)"
    def get_hash_function() -> Callable[[bytes], str]:
        return lambda data: xxhash.xxh64(data, seed=2024).hexdigest()
except ImportError:
    import hashlib
    HASH_ALGO_NAME = "hashlib (blake2b)"
    def get_hash_function() -> Callable[[bytes], str]:
        return lambda data: hashlib.blake2b(data, digest_size=32).hexdigest()

# --- Konstansok ---
DEFAULT_INPUT_DIR = Path("input") # Alapértelmezett bemeneti könyvtár
DEFAULT_OUTPUT_FILE = Path("duplicates.txt") # Alapértelmezett kimeneti fájl
LOG_DIR = Path("logs") # Naplófájlok tárolása
TEMP_DIR = Path("temp_duplicate_finder") # Ideiglenes fájlok tárolása
WRITE_LENGTH = 47   # A sorok elejének maximális hossza, amelyet kiírunk pl: "010";"HO";"1O01";"2024";"0450273881";"000002";
HASH_DELIMITER = ";"    # Elválasztó karakter a hash mezők között
HASH_FIELDS_COUNT = 6   # A hash mezők száma, amelyeket a normalizált sorokból kinyerünk
MAX_WORKERS = max(1, (os.cpu_count() or 2) - 1) # A processzorok számának csökkentése egyel, hogy elkerüljük a túlterhelést
# Memóriahasználat becslése a különböző stratégiákhoz
FAST_MODE_MEMORY_FACTOR = 0.4 # A 'fast' mód becsült memóriaigénye a fájlok teljes méretének 40%-a
SAFE_MODE_MEMORY_FACTOR = 0.1 # A 'safe' mód becsült memóriaigénye a fájlok teljes méretének 10%
RAM_USAGE_THRESHOLD = 0.60 # A RAM használatának maximális küszöbértéke a 'safe' és 'disk' módokhoz
HASH_FUNCTION = get_hash_function() # A hash függvény kiválasztása a telepített csomagok alapján

# --- Naplózás Beállítása ---
def setup_logger() -> Tuple[logging.Logger, logging.Logger]:
    LOG_DIR.mkdir(exist_ok=True)
    log_filename = LOG_DIR / f"duplicate_finder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger("IdealDuplicateFinder")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_only_logger = logging.getLogger("FileOnlyLogger")
    file_only_logger.setLevel(logging.INFO)
    file_only_logger.handlers.clear()
    file_only_logger.addHandler(file_handler)

    return logger, file_only_logger

# --- Segédfüggvények ---
def normalize_line(line: str) -> str:
    return HASH_DELIMITER.join(line.strip().split(HASH_DELIMITER, HASH_FIELDS_COUNT)[:HASH_FIELDS_COUNT])

def hash_normalized_line(line_part: str) -> str:
    return HASH_FUNCTION(line_part.encode('utf-8'))

def get_input_files(input_dir: Path, logger: logging.Logger) -> List[Path]:
    if not input_dir.is_dir():
        logger.error(f"A bemeneti könyvtár nem létezik: {input_dir}")
        return []
    
    files = [f for f in input_dir.iterdir() if f.is_file() and f.suffix.lower() in ['.csv', '.txt']]
    files.sort(key=lambda f: f.stat().st_size)
    
    if not files:
        logger.warning(f"Nincsenek feldolgozható fájlok a(z) '{input_dir}' könyvtárban.")
    
    return files

def write_duplicates(duplicates_data: Dict[str, List[str]], output_file: Path, logger: logging.Logger):
    if not duplicates_data:
        logger.info("Nem található egyetlen, több fájlban is előforduló sor sem.")
        with output_file.open('w', encoding='utf-8') as f:
            f.write("Nem található duplikátum.\n")
        return

    logger.info(f"{len(duplicates_data)} duplikált sor kiírása a(z) '{output_file}' fájlba...")
    sorted_prefixes = sorted(duplicates_data.keys())

    try:
        with output_file.open('w', encoding='utf-8') as f:
            for prefix in sorted_prefixes:
                f.write(f"{prefix}\n")
                for filename in sorted(duplicates_data[prefix]):
                    f.write(f"    - {filename}\n")
        logger.info("A duplikátumok kiírása befejeződött.")
    except IOError as e:
        logger.error(f"Hiba a kimeneti fájl írása közben: {e}")

# --- Worker Függvények (Már nem naplóznak, csak hibát dobnak) ---
def process_file_fast(file_path: Path, file_id: int) -> Dict[str, Tuple[str, Set[int]]]:
    local_hashes = {}
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f)
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue
            h = hash_normalized_line(normalize_line(stripped_line))
            if h not in local_hashes:
                local_hashes[h] = (stripped_line[:WRITE_LENGTH], {file_id})
    return local_hashes

def process_file_safe_pass1(file_path: Path) -> Set[str]:
    local_hashes = set()
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f)
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                local_hashes.add(hash_normalized_line(normalize_line(stripped_line)))
    return local_hashes

def process_file_safe_pass2(file_path: Path, duplicate_hashes: Set[str]) -> Dict[str, str]:
    results = {}
    if not duplicate_hashes: return results
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f)
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue
            h = hash_normalized_line(normalize_line(stripped_line))
            if h in duplicate_hashes and h not in results:
                results[h] = stripped_line[:WRITE_LENGTH]
    return results

def process_file_disk(file_info: Tuple[Path, int, Path]) -> Path:
    file_path, file_id, temp_dir = file_info
    temp_file_path = temp_dir / f"hashes_{file_id}.tmp"
    with file_path.open('r', encoding='utf-8', errors='ignore') as f_in, \
         temp_file_path.open('w', encoding='utf-8') as f_out:
        next(f_in)
        for line in f_in:
            stripped_line = line.strip()
            if not stripped_line: continue
            h = hash_normalized_line(normalize_line(stripped_line))
            prefix = stripped_line[:WRITE_LENGTH]
            f_out.write(f"{h}\t{file_id}\t{prefix}\n")
    return temp_file_path

# --- Stratégia Vezérlő Függvények ---
def run_strategy_fast(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: FAST (memóriaigényes) stratégia ---")
    global_hashes = defaultdict(lambda: ("", set()))
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_file_fast, path, fid): fid for fid, path in enumerate(files)}
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="FAST feldolgozás"):
            file_id = future_to_id[future]
            try:
                partial_results = future.result()
                for h, (prefix, ids) in partial_results.items():
                    if not global_hashes[h][0]:
                        global_hashes[h] = (prefix, global_hashes[h][1])
                    global_hashes[h][1].update(ids)
                file_only_logger.info(f"Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba a(z) '{id_to_file_map[file_id]}' feldolgozása közben: {e}", exc_info=True)

    output_data = {prefix: [id_to_file_map[fid] for fid in file_ids] for _, (prefix, file_ids) in global_hashes.items() if len(file_ids) > 1}
    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)
    
def run_strategy_safe(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: SAFE (memóriakímélő) stratégia ---")
    logger.info("--- 1. FÁZIS: Hash-ek gyűjtése ---")
    hash_counts = Counter()
    hash_to_files_map = defaultdict(set)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_file_safe_pass1, path): fid for fid, path in enumerate(files)}
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="SAFE 1. fázis"):
            file_id = future_to_id[future]
            try:
                hashes = future.result()
                hash_counts.update(hashes)
                for h in hashes:
                    hash_to_files_map[h].add(file_id)
                file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba (1. fázis) a(z) '{id_to_file_map[file_id]}' feldolgozása során: {e}", exc_info=True)

    duplicate_hashes = {h for h, count in hash_counts.items() if count > 1}
    if not duplicate_hashes:
        write_duplicates({}, DEFAULT_OUTPUT_FILE, logger)
        return

    logger.info(f"Összesen {len(duplicate_hashes)} egyedi, több fájlban előforduló hash azonosítva.")
    logger.info("--- 2. FÁZIS: Duplikált sorok adatainak gyűjtése ---")
    hash_to_prefix_map = {}
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(process_file_safe_pass2, path, duplicate_hashes): path for path in files}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="SAFE 2. fázis"):
            try:
                hash_to_prefix_map.update(future.result())
            except Exception as e:
                logger.error(f"Hiba (2. fázis) a(z) '{future_to_file[future].name}' feldolgozása során: {e}", exc_info=True)
    
    output_data = {prefix: [id_to_file_map[fid] for fid in hash_to_files_map[h]] for h, prefix in hash_to_prefix_map.items() if h in duplicate_hashes}
    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

def run_strategy_disk(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: DISK (diszk-alapú) stratégia ---")
    if TEMP_DIR.exists():
        for f in TEMP_DIR.iterdir(): f.unlink()
    TEMP_DIR.mkdir(exist_ok=True)

    temp_files = []
    logger.info("--- 1. FÁZIS: Hash-ek ideiglenes fájlokba írása ---")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [(path, fid, TEMP_DIR) for fid, path in enumerate(files)]
        future_to_task = {executor.submit(process_file_disk, task): task for task in tasks}
        
        for future in tqdm(as_completed(future_to_task), total=len(future_to_task), desc="DISK 1. fázis"):
            task = future_to_task[future]
            file_path, file_id = task[0], task[1]
            try:
                temp_file_path = future.result()
                file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[file_id]}")
                temp_files.append(temp_file_path)
            except Exception as e:
                logger.error(f"Hiba a(z) '{file_path.name}' (ID: {file_id}) feldolgozása során: {e}", exc_info=True)

    logger.info("--- 2. FÁZIS: Duplikátumok keresése az ideiglenes fájlokból ---")
    hash_to_prefix_map = {}
    hash_to_files_map = defaultdict(set)
    for temp_file in tqdm(temp_files, desc="DISK 2. fázis - összeolvasás"):
        try:
            with temp_file.open('r', encoding='utf-8') as f:
                for line in f:
                    try:
                        h, fid_str, prefix = line.strip().split('\t', 2)
                        hash_to_files_map[h].add(int(fid_str))
                        if h not in hash_to_prefix_map:
                            hash_to_prefix_map[h] = prefix
                    except ValueError: continue
        except Exception as e:
            logger.error(f"Hiba az ideiglenes fájl '{temp_file.name}' olvasása közben: {e}")

    output_data = {prefix: [id_to_file_map[fid] for fid in file_ids] for h, file_ids in hash_to_files_map.items() if len(file_ids) > 1 and (prefix := hash_to_prefix_map.get(h))}
    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

    logger.info("Ideiglenes fájlok törlése...")
    try:
        for f in TEMP_DIR.iterdir(): f.unlink()
        TEMP_DIR.rmdir()
    except Exception as e:
        logger.warning(f"Nem sikerült minden ideiglenes fájlt törölni: {e}")

# --- Fő Vezérlés és Stratégiaválasztás ---
def estimate_average_line_length(file: Path, logger: logging.Logger, max_lines: int = 10000) -> float:
    total_length = 0
    line_count = 0
    try:
        with file.open('r', encoding='utf-8', errors='ignore') as f:
            next(f)  # fejléc kihagyása
            for line in f:
                stripped = line.strip()
                if stripped:
                    total_length += len(stripped)
                    line_count += 1
                    if line_count >= max_lines:
                        break
        if line_count == 0:
            logger.warning(f"Nem sikerült érvényes sort találni a(z) '{file.name}' fájlban. Alapértelmezett érték lesz használva.")
            return 150.0
        return total_length / line_count
    except Exception as e:
        logger.warning(f"Hiba az átlagos sorhossz becslése közben: {e}. Alapértelmezett érték: 150.0")
        return 150.0

def auto_select_strategy(files: List[Path], logger: logging.Logger) -> str:
    if not PSUTIL_AVAILABLE:
        logger.warning("A 'psutil' csomag nem található. A 'pip install psutil' parancs futtatása javasolt.")
        logger.warning("Biztonsági okokból a 'safe' stratégia lesz használva.")
        return "safe"
    try:
        total_size_bytes = sum(f.stat().st_size for f in files)
        available_ram_bytes = psutil.virtual_memory().available
        ram_limit = available_ram_bytes * RAM_USAGE_THRESHOLD
        est_fast_mode_ram = total_size_bytes * FAST_MODE_MEMORY_FACTOR
        est_safe_mode_ram = total_size_bytes * SAFE_MODE_MEMORY_FACTOR

        # Legnagyobb fájl kiválasztása és átlagos sorhossz becslése
        largest_file = max(files, key=lambda f: f.stat().st_size)
        avg_line_length = estimate_average_line_length(largest_file, logger)

        disk_record_length = 16 + 2 + 3 + WRITE_LENGTH + 1  # hash + tabok + file_id + prefix + newline
        est_disk_space_bytes = int(total_size_bytes * (disk_record_length / avg_line_length))

        logger.info("Automatikus stratégiaválasztás:")
        logger.info(f"  - Fájlok teljes mérete: {total_size_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Rendelkezésre álló RAM: {available_ram_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Memóriaküszöb ({RAM_USAGE_THRESHOLD*100:.0f}%): {ram_limit / (1024**3):.3f} GB")
        logger.info(f"  - 'fast' mód becsült memóriaigénye: {est_fast_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'safe' mód becsült memóriaigénye: {est_safe_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'disk' mód becsült tárhelyigénye: {est_disk_space_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Átlagos sorhossz becslés a '{largest_file.name}' fájlból: {avg_line_length:.1f} karakter")

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

def main():
    parser = argparse.ArgumentParser(
        description="Párhuzamos duplikátumkereső nagy szöveges fájlokban.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-i', '--input', type=Path, default=DEFAULT_INPUT_DIR,
        help=f"Bemeneti könyvtár (alapértelmezett: '{DEFAULT_INPUT_DIR}')"
    )
    parser.add_argument(
        '-s', '--strategy', choices=['auto', 'fast', 'safe', 'disk'], default='auto',
        help="Feldolgozási stratégia."
    )
    args = parser.parse_args()

    logger, file_only_logger = setup_logger()

    start_time = time.time()
    logger.info(f"Program indítása {MAX_WORKERS} worker processzel.")
    logger.info(f"Használt hash algoritmus: {HASH_ALGO_NAME}")

    files = get_input_files(args.input, logger)
    if not files: return

    id_to_file_map = {i: f.name for i, f in enumerate(files)}

    strategy = args.strategy
    if strategy == 'auto':
        strategy = auto_select_strategy(files, logger)

    strategy_map = {
        'fast': lambda f, m: run_strategy_fast(f, m, logger, file_only_logger),
        'safe': lambda f, m: run_strategy_safe(f, m, logger, file_only_logger),
        'disk': lambda f, m: run_strategy_disk(f, m, logger, file_only_logger)
    }

    strategy_map[strategy](files, id_to_file_map)

    gc.collect()
    end_time = time.time()
    logger.info(f"A futás befejeződött. Teljes idő: {end_time - start_time:.2f} másodperc.")

if __name__ == '__main__':
    if sys.platform.startswith('win'):
        try:
            os.system('chcp 65001 > nul')
        except Exception: pass
    main()
