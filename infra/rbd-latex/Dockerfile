# Obraz rbd-latex: kompilacja biuletynu B1/B2 (LaTeX -> PDF), bez internetu w czasie kompilacji.
#
# Po co osobno: TeX Live jest duży i potrzebny tylko do składania PDF-ów, więc nie dokładamy go do rbd-python
# (ten jest na każdej maszynie dla Sparka). Pakiety Debiana: wersje stałe dla wydania bookworm.
# Polskie znaki i dzielenie wyrazów: texlive-lang-polish; czcionki Latin Modern: lmodern.
#
# Budowa (na masterze):  docker build -t rbd-latex:0.1 infra/rbd-latex
# Kompilacja:            docker run --rm -v <katalog>:/doc rbd-latex:0.1 latexmk -pdf -interaction=nonstopmode bulletin.tex
FROM debian:bookworm-20261005-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        texlive-latex-base texlive-latex-recommended texlive-latex-extra \
        texlive-fonts-recommended texlive-lang-polish lmodern latexmk \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /doc
