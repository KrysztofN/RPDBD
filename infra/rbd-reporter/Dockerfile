# Obraz rbd-reporter: raporter biuletynów B1/B2 = rbd-python (dane, wykresy) + TeX Live (PDF) + klient Kafki + kod.
#
# Po co osobny obraz: usługa Swarma na workerach z Bielikiem nie ma kodu z mastera, więc kod jest w obrazie
# (przebudowa przy zmianie kodu). TeX Live z pakietów Ubuntu (baza obrazu Sparka), polskie znaki: texlive-lang-polish.
#
# Budowa (na maszynach z Bielikiem; kontekst = katalog z kopią lib/ i B1_B2/ obok tego pliku):
#   docker build -t rbd-reporter:0.1 -f Dockerfile .
FROM rbd-python:0.1

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        texlive-latex-base texlive-latex-recommended texlive-latex-extra \
        texlive-fonts-recommended texlive-lang-polish lmodern latexmk \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m pip install --no-cache-dir confluent-kafka==2.15.1

COPY lib /app/lib
COPY B1_B2 /app/B1_B2
ENV MPLCONFIGDIR=/tmp/matplotlib
USER spark
CMD ["python3", "-m", "B1_B2.reporter.reporter"]
