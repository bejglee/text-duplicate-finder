import os
import time
import xxhash  # Gyorsabb hash algoritmus
import logging
import argparse # Parancssori argumentumok kezeléséhez
import psutil  # Rendszerinformációk (pl. RAM) lekérdezéséhez
from datetime import datetime
from collections import defaultdict, Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Telepítési útmutató ---
# A program futtatásához telepíteni kell a psutil és xxhash csomagokat:
# pip install psutil xxhash

# ==============================================================================
# Használat a parancssorból
# ==============================================================================
#
# 1. Előfeltételek telepítése:
#    A program futtatásához telepíteni kell a szükséges csomagokat:
#    pip install psutil xxhash
#
# 2. Futtatási parancsok:
#    A feldolgozási stratégia a '--strategy' kapcsolóval választható meg.
#
#    - Automatikus mód (alapértelmezett):
#      A program a fájlméret és a szabad RAM alapján választja ki a
#      leghatékonyabb módszert.
#
#      python program_neve.py
#      (vagy: python program_neve.py --strategy auto)
#
#    - Gyors mód (memóriaigényes) kényszerítése:
#      Akkor javasolt, ha a fájlok mérete nem túl nagy és van elég RAM.
#
#      python program_neve.py --strategy fast
#
#    - Biztonságos mód (memóriatakarékos) kényszerítése:
#      Nagy fájlok vagy kevés RAM esetén javasolt, mivel kevés memóriát
#      használ, de a futási idő hosszabb lehet.
#
#      python program_neve.py --strategy safe
#
# ==============================================================================

# --- Konstansok ---
INPUT_DIR = "input"
OUTPUT_FILE = "duplicates.txt"
LOG_DIR = "logs"
WRITE_LENGTH = 47   # sor: "010";"HO";"1O01";"2024";"0450273881";"000002";
MAX_WORKERS = (os.cpu_count() - 1) or 4
HASH_FUNCTION = lambda data: xxhash.xxh64(data, seed=2024).hexdigest()

# --- Heurisztika konstansok ---
# Becslés: a hash térkép a RAM-ban kb. a fájlok méretének 25%-át foglalja el.
# Ez egy nagyon durva becslés, a sorok egyediségétől függ.
ESTIMATED_MEMORY_USAGE_FACTOR = 0.25 
# Ha a becsült memóriaigény meghaladja a szabad RAM 75%-át, váltsunk biztonságos módra.
AVAILABLE_RAM_THRESHOLD = 0.75

# --- Logger beállítása (változatlan) ---
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(
        LOG_DIR,
        f"duplicatefinder{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logger = logging.getLogger("DuplicateFinder")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

LOGGER = setup_logger()

# --- Közös segédfüggvények ---
def normalize_line(line: str) -> str:
    return ";".join(line.split(";")[:6])

def hash_line(line_part: str) -> str:
    return HASH_FUNCTION(line_part.encode('utf-8'))

def write_duplicates(duplicates_data: dict, output_file: str):
    """
    Univerzális kiíró függvény.
    A 'duplicates_data' formátuma: { prefix: [filename1, filename2, ...] }
    """
    LOGGER.info(f"Duplikátumok kiírása a(z) {output_file} fájlba...")
    sorted_prefixes = sorted(duplicates_data.keys())
    with open(output_file, 'w', encoding='utf-8') as f:
        for prefix in sorted_prefixes:
            f.write(f"{prefix}\n")
            for filename in sorted(duplicates_data[prefix]):
                f.write(f"    - {filename}\n")
    LOGGER.info("A duplikátumok kiírása befejeződött.")

# ==============================================================================
# --- 1. STRATÉGIA: GYORS (MEMÓRIAIGÉNYES, EGYFÁZISÚ) MEGOLDÁS ---
# ==============================================================================
def _fast_default_hash_factory():
    return (None, set())

def _fast_process_file(file_path: str, file_id: int):
    local_hashes = defaultdict(_fast_default_hash_factory)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            next(f)
            for line in f:
                stripped_line = line.strip()
                if not stripped_line: continue
                normalized = normalize_line(stripped_line)
                h = hash_line(normalized)
                line_prefix, file_ids = local_hashes[h]
                if line_prefix is None:
                    line_prefix = stripped_line[:WRITE_LENGTH]
                file_ids.add(file_id)
                local_hashes[h] = (line_prefix, file_ids)
    except Exception as e:
        LOGGER.error(f"Hiba a(z) {file_path} fájl feldolgozása közben: {e}")
    LOGGER.info(f"'Gyors' feldolgozás kész: {os.path.basename(file_path)}, {len(local_hashes)} egyedi hash.")
    return local_hashes

def run_strategy_fast_in_memory(files_to_process: list, id_file_map: dict):
    LOGGER.info("Indítás 'gyors (memóriaigényes)' stratégiával.")
    all_results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fast_process_file, path, i): path for i, path in enumerate(files_to_process)}
        for future in as_completed(futures):
            try:
                all_results.append(future.result())
            except Exception as e:
                LOGGER.error(f"Hiba a(z) {futures[future]} feldolgozása során: {e}")

    LOGGER.info("Hash térképek egyesítése...")
    global_hashes = defaultdict(_fast_default_hash_factory)
    for local_map in all_results:
        for h, (prefix, file_ids) in local_map.items():
            if global_hashes[h][0] is None:
                global_hashes[h] = (prefix, global_hashes[h][1])
            global_hashes[h][1].update(file_ids)

    LOGGER.info("Duplikátumok keresése és kimenet előkészítése...")
    output_data = defaultdict(list)
    for h, (prefix, file_ids) in global_hashes.items():
        if len(file_ids) > 1:
            output_data[prefix] = [id_file_map[fid] for fid in file_ids]
    
    num_duplicates = len(output_data)
    LOGGER.info(f"Összesen {num_duplicates} sor fordult elő több fájlban.")
    if num_duplicates > 0:
        write_duplicates(output_data, OUTPUT_FILE)

# ==============================================================================
# --- 2. STRATÉGIA: BIZTONSÁGOS (MEMÓRIATAKARÉKOS, KÉTFÁZISÚ) MEGOLDÁS ---
# ==============================================================================
def _safe_phase1_process_file(file_path: str):
    hashes = set()
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            next(f)
            for line in f:
                stripped_line = line.strip()
                if not stripped_line: continue
                h = hash_line(normalize_line(stripped_line))
                hashes.add(h)
    except Exception as e:
        LOGGER.error(f"Hiba az 1. fázisban ({file_path}): {e}")
    return hashes

def _safe_phase2_process_file(file_path: str, duplicate_hashes: set):
    results = {}
    if not duplicate_hashes: return results
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            next(f)
            for line in f:
                stripped_line = line.strip()
                if not stripped_line: continue
                h = hash_line(normalize_line(stripped_line))
                if h in duplicate_hashes:
                    results[h] = stripped_line[:WRITE_LENGTH]
                    duplicate_hashes.remove(h)
                    if not duplicate_hashes: break
    except Exception as e:
        LOGGER.error(f"Hiba a 2. fázisban ({file_path}): {e}")
    return results

def run_strategy_safe_two_pass(files_to_process: list, id_file_map: dict):
    LOGGER.info("Indítás 'biztonságos (memóriatakarékos)' stratégiával.")
    file_path_map = {i: path for i, path in enumerate(files_to_process)}
    
    # 1. FÁZIS
    LOGGER.info("--- 1. FÁZIS: Hash-ek gyűjtése ---")
    all_hashes_by_file_id = defaultdict(set)
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(_safe_phase1_process_file, path): i for i, path in file_path_map.items()}
        for future in as_completed(future_to_id):
            file_id = future_to_id[future]
            try:
                all_hashes_by_file_id[file_id] = future.result()
            except Exception as e:
                LOGGER.error(f"Hiba a(z) {id_file_map[file_id]} feldolgozása során (1. fázis): {e}")

    hash_counts = Counter(h for hashes in all_hashes_by_file_id.values() for h in hashes)
    duplicate_hashes = {h for h, count in hash_counts.items() if count > 1}
    num_duplicates = len(duplicate_hashes)
    LOGGER.info(f"Összesen {num_duplicates} egyedi sor fordult elő több fájlban.")
    if num_duplicates == 0: return

    hash_to_file_ids = defaultdict(list)
    for file_id, hashes in all_hashes_by_file_id.items():
        for h in hashes:
            if h in duplicate_hashes:
                hash_to_file_ids[h].append(file_id)
                
    # 2. FÁZIS
    LOGGER.info("--- 2. FÁZIS: Duplikátum sorok adatainak kinyerése ---")
    hashes_to_find_by_file = defaultdict(set)
    for h in duplicate_hashes:
        for file_id in hash_to_file_ids[h]:
            hashes_to_find_by_file[file_id].add(h)
            
    hash_to_prefix_map = {}
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_safe_phase2_process_file, file_path_map[fid], hashes) for fid, hashes in hashes_to_find_by_file.items()}
        for future in as_completed(futures):
            try:
                hash_to_prefix_map.update(future.result())
            except Exception as e:
                LOGGER.error(f"Hiba a 2. fázisban: {e}")
    
    # Kimenet összeállítása
    output_data = {prefix: [id_file_map[fid] for fid in hash_to_file_ids[h]] for h, prefix in hash_to_prefix_map.items()}
    write_duplicates(output_data, OUTPUT_FILE)

# ==============================================================================
# --- FŐ VEZÉRLÉS ÉS STRATÉGIAVÁLASZTÁS ---
# ==============================================================================
def choose_strategy_automatically(files_to_process: list) -> str:
    """Heurisztika alapján eldönti, melyik stratégiát használja."""
    try:
        total_size_bytes = sum(os.path.getsize(f) for f in files_to_process)
        available_ram_bytes = psutil.virtual_memory().available

        estimated_memory_need = total_size_bytes * ESTIMATED_MEMORY_USAGE_FACTOR
        ram_limit = available_ram_bytes * AVAILABLE_RAM_THRESHOLD

        LOGGER.info(f"Automatikus stratégiaválasztás:")
        LOGGER.info(f"  - Fájlok teljes mérete: {total_size_bytes / (1024**3):.2f} GB")
        LOGGER.info(f"  - Becsült memóriaigény ('gyors' mód): {estimated_memory_need / (1024**3):.2f} GB")
        LOGGER.info(f"  - Rendelkezésre álló RAM: {available_ram_bytes / (1024**3):.2f} GB (a limit: {ram_limit / (1024**3):.2f} GB)")

        if estimated_memory_need > ram_limit:
            LOGGER.info("Döntés: A becsült memóriaigény magas. A 'biztonságos' (safe) stratégia lesz használva.")
            return "safe"
        else:
            LOGGER.info("Döntés: Elegendő memória áll rendelkezésre. A 'gyors' (fast) stratégia lesz használva.")
            return "fast"
            
    except Exception as e:
        LOGGER.warning(f"Nem sikerült az automatikus stratégiaválasztás ({e}). Alapértelmezett: 'biztonságos' mód.")
        return "safe"

def main():
    parser = argparse.ArgumentParser(description="Duplikátumokat keres több szöveges fájlban.")
    parser.add_argument(
        '--strategy',
        choices=['auto', 'fast', 'safe'],
        default='auto',
        help="A feldolgozási stratégia. 'auto': automatikus választás memória alapján. "
             "'fast': gyorsabb, de memóriaigényes. 'safe': lassabb, de memóriatakarékos."
    )
    args = parser.parse_args()

    start_time = time.time()
    LOGGER.info(f"Program indítása '{args.strategy}' stratégiával...")

    if not os.path.isdir(INPUT_DIR):
        LOGGER.error(f"A(z) '{INPUT_DIR}' mappa nem található!")
        return

    files_to_process = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))]
    if not files_to_process:
        LOGGER.warning(f"Nincsenek feldolgozandó fájlok a(z) '{INPUT_DIR}' mappában.")
        return

    LOGGER.info(f"{len(files_to_process)} fájl feldolgozása {MAX_WORKERS} szálon...")
    id_file_map = {i: os.path.basename(f) for i, f in enumerate(files_to_process)}
    
    strategy = args.strategy
    if strategy == 'auto':
        strategy = choose_strategy_automatically(files_to_process)
    
    if strategy == 'fast':
        run_strategy_fast_in_memory(files_to_process, id_file_map)
    elif strategy == 'safe':
        run_strategy_safe_two_pass(files_to_process, id_file_map)

    end_time = time.time()
    LOGGER.info(f"A program futása befejeződött. Időtartam: {end_time - start_time:.2f} másodperc.")

if __name__ == "__main__":
    main()