"""
Krok wspólny dla B1 i B2: powiaty i struktura upraw (EUCROPMAP).
Leży w B1_B2/ (kod tylko biuletynu); funkcje dla całego zespołu są w lib/.

Co robi:
  1. pobiera granice powiatów z GUGiK (raz, potem z pliku) i odrzuca miasta na prawach powiatu,
  2. dla każdego rocznika EUCROPMAP liczy udział grup upraw w kwadratach 250 m (raz, potem z pliku),
  3. zapisuje dla 314 powiatów ziemskich:
       data/crops/crop_structure_counties_<rok>.parquet  — km2 i % pól ornych każdej grupy upraw
       data/crops/pure_pixels_counties_<rok>.parquet     — ile pikseli MODIS to „czysta” grupa upraw
  4. w logu podsumowuje, w ilu powiatach da się liczyć osobną krzywą NDVI dla grupy upraw.

Teoria: patrz nagłówki lib/eucropmap.py i lib/boundaries.py. Wykresy i komentarz do wyników: notebooks/07.

Uruchomienie (z katalogu głównego repo):
  python -m B1_B2.prepare_counties_crops                 # domyślnie lata 2018 i 2022
  python -m B1_B2.prepare_counties_crops --years 2018 -v
"""
from __future__ import annotations

import argparse
from pathlib import Path

from lib import boundaries
from lib import eucropmap as ec
from lib import ndvi_tools as nt
from lib.log import Timer, get_logger, setup_logging

ROOT = Path(__file__).resolve().parents[1]
log = get_logger(Path(__file__).stem)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Powiaty + struktura upraw EUCROPMAP (krok wspólny B1 + B2)")
    p.add_argument("--years", type=int, nargs="+", default=sorted(ec.FILES), choices=sorted(ec.FILES),
                   help="roczniki EUCROPMAP (domyślnie wszystkie)")
    p.add_argument("--data-dir", type=Path, default=ROOT / "data")
    p.add_argument("--threshold", type=float, default=0.75,
                   help="udział grupy w pikselu MODIS, od którego piksel jest „czysty”")
    p.add_argument("--min-pixels", type=int, default=50,
                   help="min. liczba czystych pikseli, by liczyć krzywą grupy w powiecie")
    p.add_argument("--overwrite", action="store_true", help="policz mapę upraw od nowa, nawet jeśli plik istnieje")
    p.add_argument("-v", "--verbose", action="store_true", help="szczegółowe logi (DEBUG)")
    p.add_argument("-q", "--quiet", action="store_true", help="tylko ostrzeżenia i błędy")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.quiet)
    out_dir = args.data_dir / "crops"
    out_dir.mkdir(parents=True, exist_ok=True)

    with Timer(log, "granice powiatów"):
        counties = boundaries.load_counties(args.data_dir)
        rural = counties[~counties["is_city"]].reset_index(drop=True)
        log.info("powiaty: %d, w tym ziemskich %d (miasta na prawach powiatu pomijamy)", len(counties), len(rural))

    grids = None
    for year in args.years:
        with Timer(log, f"EUCROPMAP {year}"):
            path = ec.build_group_fractions(year, counties, args.data_dir, overwrite=args.overwrite)

            structure = ec.crop_structure(path, rural)
            structure = rural[["region_id", "name", "voiv_name"]].merge(structure, on="region_id")
            structure_file = out_dir / f"crop_structure_counties_{year}.parquet"
            structure.to_parquet(structure_file, index=False)
            log.info("struktura upraw -> %s (pola orne razem: %.0f km2)", structure_file, structure["arable_km2"].sum())

            if grids is None:
                grids = nt.modis_tile_grids(year)
                log.debug("kafelki MODIS: %s", [g[0] for g in grids])
            pure = ec.pure_pixel_counts(path, rural, grids, thresholds=(args.threshold,))
            pure_file = out_dir / f"pure_pixels_counties_{year}.parquet"
            pure.to_parquet(pure_file, index=False)
            log.info("czyste piksele -> %s", pure_file)

            ok = pure[pure["n_pixels"] >= args.min_pixels].groupby("group").size()
            summary = ", ".join(f"{label} {ok.get(key, 0)}" for key, label, _ in ec.GROUPS.values())
            log.info("powiaty z krzywą grupy (>= %d pikseli, udział >= %.0f%%) na %d: %s",
                     args.min_pixels, 100 * args.threshold, len(rural), summary)


if __name__ == "__main__":
    main()
