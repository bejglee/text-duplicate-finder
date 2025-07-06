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
import heapq
import logging
import argparse
import itertools
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, Set, List, Tuple, Callable, Iterator
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
# Konfigurációs konstansok
DEFAULT_INPUT_DIR = Path("input")
DEFAULT_OUTPUT_FILE = Path("duplicates.txt")
LOG_DIR = Path("logs")
TEMP_DIR = Path("temp_duplicate_finder")
MAX_WORKERS = max(1, (os.cpu_count() or 2) - 1)

# Sor feldolgozási konstansok
WRITE_LENGTH = 47
HASH_DELIMITER = ";"
HASH_FIELDS_COUNT = 6
DISK_MODE_DELIMITER = "\t" # Biztonságosabb, ha a prefix nem tartalmazhatja

# Stratégiaválasztási konstansok
FAST_MODE_MEMORY_FACTOR = 0.4
SAFE_MODE_MEMORY_FACTOR = 0.1
RAM_USAGE_THRESHOLD = 0.70
HASH_FUNCTION = get_hash_function()

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
        logger.info("Nem található duplikált sor (sem fájlon belül, sem fájlok között).")
        with output_file.open('w', encoding='utf-8') as f:
            f.write("Nem található duplikátum.\n")
        return

    logger.info(f"{len(duplicates_data)} duplikált sor kiírása a(z) '{output_file}' fájlba...")
    sorted_prefixes = sorted(duplicates_data.keys())

    try:
        with output_file.open('w', encoding='utf-8') as f:
            for prefix in sorted_prefixes:
                f.write(f"{prefix}\n")
                # A kimenet egyértelműsítése, ha csak egy fájlban van a duplikátum
                if len(duplicates_data[prefix]) == 1:
                    f.write(f"    - (Fájlon belüli duplikátumok) {duplicates_data[prefix][0]}\n")
                else:
                    for filename in sorted(duplicates_data[prefix]):
                        f.write(f"    - {filename}\n")
        logger.info("A duplikátumok kiírása befejeződött.")
    except IOError as e:
        logger.error(f"Hiba a kimeneti fájl írása közben: {e}")

# --- Worker Függvények ---

def process_file_fast(file_path: Path, file_id: int) -> Dict[str, Tuple[str, int]]:
    local_hashes = {}  # hash -> [prefix, count]
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue
            
            h = hash_normalized_line(normalize_line(stripped_line))
            
            if h not in local_hashes:
                local_hashes[h] = [stripped_line[:WRITE_LENGTH], 1]
            else:
                local_hashes[h][1] += 1
    
    # Átalakítás a visszatérési típusnak megfelelően: Dict[hash, (prefix, count)]
    return {h: (data[0], data[1]) for h, data in local_hashes.items()}

def process_file_safe_pass1(file_path: Path) -> Counter:
    local_hashes = Counter()
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                local_hashes[hash_normalized_line(normalize_line(stripped_line))] += 1
    return local_hashes

def process_file_safe_pass2(file_path: Path, duplicate_hashes: Set[str]) -> Dict[str, str]:
    results = {}
    if not duplicate_hashes: return results
    with file_path.open('r', encoding='utf-8', errors='ignore') as f:
        next(f, None)
        for line in f:
            stripped_line = line.strip()
            if not stripped_line: continue
            h = hash_normalized_line(normalize_line(stripped_line))
            if h in duplicate_hashes and h not in results:
                results[h] = stripped_line[:WRITE_LENGTH]
    return results

def process_and_sort_file_disk(file_info: Tuple[Path, int, Path]) -> Path:
    file_path, file_id, temp_dir = file_info
    temp_file_path = temp_dir / f"hashes_{file_id}.tmp"
    
    lines = []
    with file_path.open('r', encoding='utf-8', errors='ignore') as f_in:
        next(f_in, None)
        for line in f_in:
            stripped_line = line.strip()
            if not stripped_line:
                continue
            prefix = stripped_line[:WRITE_LENGTH].replace(DISK_MODE_DELIMITER, " ")
            normalized_line = normalize_line(stripped_line)
            if not normalized_line:
                continue
            h = hash_normalized_line(normalized_line)
            lines.append((h, file_id, prefix))
    
    lines.sort(key=lambda x: x[0])
    
    lines_to_write = [f"{h}{DISK_MODE_DELIMITER}{fid}{DISK_MODE_DELIMITER}{pref}\n" for h, fid, pref in lines]
    with temp_file_path.open('w', encoding='utf-8') as f_out:
        f_out.writelines(lines_to_write)
    
    return temp_file_path

# --- Stratégia Vezérlő Függvények ---
def run_strategy_fast(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: FAST (memóriaigényes) stratégia ---")
    # hash -> (prefix, Counter({file_id: count}))
    global_hashes = defaultdict(lambda: ("", Counter()))

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_file_fast, path, fid): fid for fid, path in enumerate(files)}
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="FAST feldolgozás"):
            file_id = future_to_id[future]
            try:
                # partial_results formátuma: {hash: (prefix, count)}
                partial_results = future.result()
                for h, (prefix, count) in partial_results.items():
                    # Prefix elmentése, ha még nincs
                    if not global_hashes[h][0]:
                        global_hashes[h] = (prefix, global_hashes[h][1])
                    # Számláló frissítése az adott fájlhoz
                    global_hashes[h][1][file_id] = count
                file_only_logger.info(f"Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba a(z) '{id_to_file_map[file_id]}' feldolgozása közben: {e}", exc_info=True)

    # Duplikátumok szűrése a teljes előfordulási szám alapján
    output_data = {}
    for _, (prefix, file_counts) in global_hashes.items():
        # Akkor duplikátum, ha a teljes előfordulási száma > 1
        if sum(file_counts.values()) > 1:
            # A fájlneveket gyűjtjük, ahol a hash előfordult
            file_ids = file_counts.keys()
            output_data[prefix] = [id_to_file_map[fid] for fid in sorted(list(file_ids))]

    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

def run_strategy_safe(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: SAFE (memóriakímélő) stratégia ---")
    logger.info("--- 1. FÁZIS: Hash-ek és előfordulásaik gyűjtése ---")
    
    # hash -> Counter({file_id: count})
    hash_to_file_counts = defaultdict(Counter)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(process_file_safe_pass1, path,): fid for fid, path in enumerate(files)}
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id), desc="SAFE 1. fázis"):
            file_id = future_to_id[future]
            try:
                # hashes_with_counts egy Counter objektum: {hash: count}
                hashes_with_counts = future.result()
                for h, count in hashes_with_counts.items():
                    hash_to_file_counts[h][file_id] = count
                file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[file_id]}")
            except Exception as e:
                logger.error(f"Hiba (1. fázis) a(z) '{id_to_file_map[file_id]}' feldolgozása során: {e}", exc_info=True)

    # Duplikátumok szűrése a teljes előfordulási szám alapján
    duplicate_hashes = {
        h for h, file_counts in hash_to_file_counts.items()
        if sum(file_counts.values()) > 1
    }
    
    if not duplicate_hashes:
        write_duplicates({}, DEFAULT_OUTPUT_FILE, logger)
        return

    logger.info(f"Összesen {len(duplicate_hashes)} egyedi duplikált hash azonosítva.")
    logger.info("--- 2. FÁZIS: Duplikált sorok adatainak gyűjtése ---")
    hash_to_prefix_map = {}
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(process_file_safe_pass2, path, duplicate_hashes): path for path in files}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="SAFE 2. fázis"):
            try:
                hash_to_prefix_map.update(future.result())
            except Exception as e:
                logger.error(f"Hiba (2. fázis) a(z) '{future_to_file[future].name}' feldolgozása során: {e}", exc_info=True)
    
    # Kimenet összeállítása a módosított adatszerkezet alapján
    output_data = {}
    for h, prefix in hash_to_prefix_map.items():
        if h in duplicate_hashes:
            file_ids = hash_to_file_counts[h].keys()
            output_data[prefix] = [id_to_file_map[fid] for fid in sorted(list(file_ids))]

    write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

def run_strategy_disk(files: List[Path], id_to_file_map: Dict[int, str], logger: logging.Logger, file_only_logger: logging.Logger):
    logger.info("--- Indítás: DISK (valódi diszk-alapú) stratégia ---")
    if TEMP_DIR.exists():
        for f in TEMP_DIR.iterdir(): f.unlink()
    TEMP_DIR.mkdir(exist_ok=True)

    temp_files = []
    try:
        logger.info("--- 1. FÁZIS: Adatok feldolgozása és rendezett ideiglenes fájlokba írása ---")
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [(path, fid, TEMP_DIR) for fid, path in enumerate(files)]
            future_to_task = {executor.submit(process_and_sort_file_disk, task): task for task in tasks}
            
            for future in tqdm(as_completed(future_to_task), total=len(future_to_task), desc="DISK 1. fázis"):
                task_path, task_id = future_to_task[future][0], future_to_task[future][1]
                try:
                    temp_file_path = future.result()
                    temp_files.append(temp_file_path)
                    file_only_logger.info(f"[1. fázis] Feldolgozva: {id_to_file_map[task_id]}")
                except Exception as e:
                    logger.error(f"Hiba a(z) '{task_path.name}' (ID: {task_id}) feldolgozása során: {e}", exc_info=True)

        logger.info("--- 2. FÁZIS: Rendezett fájlok összefésülése és duplikátumok keresése (alacsony memóriaigénnyel) ---")
        output_data = {}
        
        open_files = [f.open('r', encoding='utf-8') for f in temp_files]
        merged_lines = heapq.merge(*open_files)
        line_grouper = itertools.groupby(merged_lines, key=lambda line: line.split(DISK_MODE_DELIMITER, 1)[0])

        for h, group in tqdm(line_grouper, desc="DISK 2. fázis - Fájlok összefésülése"):
            group_items = list(group)
            
            # A feltétel egyszerűsítése. Ha egy hash-hez több sor tartozik, az duplikátum.
            if len(group_items) > 1:
                file_ids = set()
                prefix = ""
                for item in group_items:
                    try:
                        _, fid_str, current_prefix = item.strip().split(DISK_MODE_DELIMITER, 2)
                        file_ids.add(int(fid_str))
                        if not prefix: 
                            prefix = current_prefix
                    except ValueError:
                        continue
                
                # Ha találtunk prefixet, hozzáadjuk az eredményhez a kapcsolódó fájlnevekkel.
                if prefix:
                    output_data[prefix] = [id_to_file_map[fid] for fid in sorted(list(file_ids))]

        for f in open_files:
            f.close()

        write_duplicates(output_data, DEFAULT_OUTPUT_FILE, logger)

    finally:
        logger.info("Ideiglenes fájlok törlése...")
        try:
            for f in TEMP_DIR.iterdir():
                f.unlink()
            TEMP_DIR.rmdir()
        except Exception as e:
            logger.warning(f"Nem sikerült minden ideiglenes fájlt törölni: {e}")


# --- Fő Vezérlés és Stratégiaválasztás ---
def estimate_average_line_length(file: Path, logger: logging.Logger, max_lines: int = 10000) -> float:
    total_length = 0
    line_count = 0
    try:
        with file.open('r', encoding='utf-8', errors='ignore') as f:
            next(f, None)
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

        logger.info("Automatikus stratégiaválasztás:")
        logger.info(f"  - Fájlok teljes mérete: {total_size_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Rendelkezésre álló RAM: {available_ram_bytes / (1024**3):.3f} GB")
        logger.info(f"  - Memóriaküszöb ({RAM_USAGE_THRESHOLD*100:.0f}%): {ram_limit / (1024**3):.3f} GB")
        logger.info(f"  - 'fast' mód becsült memóriaigénye: {est_fast_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'safe' mód becsült memóriaigénye: {est_safe_mode_ram / (1024**3):.3f} GB")
        logger.info(f"  - 'disk' mód becsült ideiglenes tárhelyigénye: ~{total_size_bytes / (1024**3):.3f} GB")
        
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
        description="Párhuzamos duplikátumkereső nagy szöveges fájlokban. Képes a fájlon belüli és fájlok közötti duplikátumok megtalálására.",
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
    if not files:
        return

    id_to_file_map = {i: f.name for i, f in enumerate(files)}

    strategy = args.strategy
    if strategy == 'auto':
        strategy = auto_select_strategy(files, logger)

    strategy_map = {
        'fast': run_strategy_fast,
        'safe': run_strategy_safe,
        'disk': run_strategy_disk,
    }

    strategy_map[strategy](files, id_to_file_map, logger, file_only_logger)

    # gc.collect()
    end_time = time.time()
    logger.info(f"A futás befejeződött. Teljes idő: {end_time - start_time:.2f} másodperc.")

if __name__ == '__main__':
    if sys.platform.startswith('win'):
        try:
            os.system('chcp 65001 > nul')
        except Exception: 
            pass
    main()