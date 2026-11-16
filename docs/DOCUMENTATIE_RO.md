# MatchPulse: documentația părții lui David (persoana 1)

Proiect de echipă, MSc Big Data Processing and Applications.
**David**: Kafka, replay producer, contracte, workflow-uri, Streamlit ·
**Mihai**: date istorice, Delta Lake pe MinIO, Spark batch ·
**Dayana**: Spark Structured Streaming, Redis, Grafana, detecție de anomalii.

---

## 1. Pe scurt

MatchPulse este o platformă de analiză a meciurilor de fotbal, construită după **arhitectura
Lambda**, cu două căi de procesare:

```
 Calea rapidă (speed):
 StatsBomb JSON ──► replay-producer ──► Kafka ──► Spark Streaming ──► Redis ──► Grafana
                    (David)             (David)   (Dayana)                      (Dayana)

 Calea batch:
 StatsBomb + Kaggle ──► Spark batch ──► Delta Lake pe MinIO ──► Streamlit
                        (Mihai)         (bronze/silver/gold)    (David)
```

- **Replay producer-ul** citește evenimentele reale ale unui meci (date StatsBomb Open Data)
  și le trimite în Kafka **în ritmul meciului**, ca și cum meciul s-ar juca live.
- **Streamlit** afișează statistici istorice (echipe, jucători, ligi) din tabelele *gold*.
- Totul rulează local, cu Docker, pornit cu o singură comandă: `make up`.

## 2. Cum pornești totul

```bash
make up                                   # pornește Kafka, Kafka UI, MinIO, Streamlit
make health                               # verifică fiecare serviciu (OK/FAIL)
make fetch-data                           # descarcă datele StatsBomb în data/
make replay-match MATCH=3788741 SPEED=60  # trimite un meci în Kafka (~2 min la 60x)
make test-replay MATCH=3788741            # verifică: complet și în ordine? (PASS/FAIL)
make replay-many                          # 3 meciuri în paralel, cu log de throughput
make dev-fixtures                         # tabele gold de test pentru Streamlit
make down                                 # oprește totul
```

Adrese: Kafka UI http://localhost:8080 · Streamlit http://localhost:8501 ·
MinIO http://localhost:9001 (minioadmin/minioadmin) · Kafka de pe laptop `localhost:9094`.
Fără `make` (Windows/WSL): `bash scripts/<nume>.sh`.

---

## 3. Faza 1: repository și mediul de lucru

**Ce am făcut**
- Structura proiectului: `producer/`, `batch/` (Mihai), `streaming/` (Dayana),
  `dashboards/streamlit/` (David), `dashboards/grafana/` (Dayana), `scripts/`, `docs/`,
  `data/` (ignorat de git). Fiecare folder are un README cu proprietarul și scopul lui.
- `docker-compose.yml` cu **Kafka 4.1 în mod KRaft** (fără ZooKeeper), **Kafka UI** și un
  container `kafka-init` care creează automat topic-urile. Toate imaginile au versiuni fixe
  și funcționează pe arm64 (Mac M3) și pe amd64 (Intel/WSL).
- **Două contracte**, adică acordurile cu colegii:
  - `docs/event-schema.md` + JSON Schema: formatul exact al mesajelor din Kafka și de unde
    vine fiecare câmp din datele StatsBomb (de ex. `xg ← shot.statsbomb_xg`);
  - `docs/gold-contract.md`: tabelele gold pe care Streamlit le așteaptă de la Mihai
    (propunere, de agreat cu el).

**Cum funcționează**
- Kafka are **două „uși" (listeners)**: `kafka:9092` pentru containere și `localhost:9094`
  pentru scripturile rulate de pe laptop.
- Topic-uri:

| Topic | Partiții | Cheie | Conținut |
|---|---|---|---|
| `match-events` | 6 | `match_id` | fiecare eveniment din meci (pasă, șut, fault...) |
| `match-control` | 1 | `match_id` | semnale: kick_off, half_time, second_half_start, full_time |
| `match-alerts` | 3 | `match_id` | alertele scrise de Dayana |

- **De ce cheia este `match_id`:** Kafka trimite toate mesajele cu aceeași cheie în aceeași
  partiție, iar într-o partiție ordinea este garantată. Deci evenimentele unui meci ajung la
  consumator exact în ordinea în care au fost trimise.

## 4. Faza 2: replay producer-ul

**Ce am făcut** (în `producer/`)

| Fișier | Rol |
|---|---|
| `transform.py` | transformă un eveniment StatsBomb brut în formatul din contract |
| `schedule.py` | stabilește ordinea, pauzele dintre evenimente și semnalele de control |
| `loader.py` | citește meciul din `data/`; dacă lipsește, îl descarcă cu `statsbombpy` |
| `replay.py` | programul principal: trimite în Kafka, cu argumentele `--matches`, `--speed`, `--bootstrap` |
| `test_consumer.py` | citește meciul înapoi din Kafka și verifică dacă e complet și în ordine |
| `tests/` | 26 de teste unitare (pytest) pe un meci mic, construit manual (19 evenimente) |
| `Dockerfile` | imaginea pentru serviciul `replay-producer` (pornit doar la cerere) |

**Cum funcționează**
1. Evenimentele se sortează după **(perioadă, index)**.
2. Înainte de fiecare eveniment, producer-ul așteaptă diferența de timp reală față de
   evenimentul anterior, **împărțită la viteză** (la `--speed 60`, un minut de meci durează
   o secundă). Pauza de la jumătate (15 minute) devine 15 secunde.
3. La începutul și la sfârșitul fiecărei reprize se trimit semnale în `match-control`.
   Înainte de `full_time`, producer-ul face *flush*, ca toate evenimentele să fie livrate
   înainte de semnalul de final.
4. Fiecare replay primește un **`replay_id`** unic, ca două rulări ale aceluiași meci să nu
   se amestece.
5. Producer-ul este **idempotent**: dacă un mesaj trebuie retrimis, Kafka nu creează
   duplicate și nu schimbă ordinea. Ctrl+C oprește curat, după mesajul curent.

**Decizie importantă:** datele StatsBomb conțin **timestamp-uri greșite** (10 în cele 13
meciuri descărcate; de exemplu, o recepție de minge din minutul 45 apare cu ora 00:00:00).
Dacă sortam după timestamp, evenimentul ar fi sărit la începutul meciului. De aceea sortăm
după `index` (ordinea proprie StatsBomb), iar pauzele le calculăm față de cel mai mare timp
văzut până atunci, așa că nu apar nici pauze negative, nici pauze uriașe.

**Rezultat verificat:** meciul Turcia–Italia (3.803 evenimente) la 60x a durat 110 secunde,
cu 0 erori. `test_consumer` a dat PASS la toate cele 7 verificări: număr, duplicate, aceleași
evenimente, ordine, semnale, total la `full_time`, JSON Schema.

## 5. Faza 3: scalare, workflow-uri, Streamlit

**Ce am făcut**
- **Replay paralel:** un fir de execuție (thread) pe meci, toate folosind același producer
  Kafka. La fiecare 5 secunde se afișează throughput-ul (evenimente/secundă, total, progres
  pe meci). Dacă un meci are o eroare, celelalte continuă.
- **`Makefile` + `scripts/*.sh`:** fiecare comandă `make` apelează un script, deci colegii
  pe Windows/WSL pot rula aceleași comenzi fără `make`. `make load-history` și
  `make run-batch` apelează scripturile lui Mihai; până există, afișează „not implemented yet".
- **Aplicația Streamlit** (`dashboards/streamlit/`):
  - `data_access.py`: **singurul loc** care știe unde sunt tabelele. Citește tabele Delta
    din MinIO cu `deltalake` (delta-rs), **fără Spark**;
  - pagina 1, **Team profile**: victorii/egaluri/înfrângeri, goluri vs xG, acasă vs deplasare,
    comparație cu media competiției, clasament;
  - pagina 2, **Player shot map**: fiecare șut al jucătorului pe teren, mărimea cercului =
    xG, golurile pline și ratările goale;
  - `dev_fixtures.py`: construiește **tabele de test** (marcate „DEV DATA" în aplicație) cu
    exact coloanele din contract, până când Mihai livrează tabelele reale.
- **MinIO:** am adăugat un serviciu MinIO de pornire, ca Streamlit să poată fi testat.
  Mihai îl preia și îl poate modifica.

**Cum funcționează datele de test**
- Rezultatele (clasamente, tendințe) vin din listele de meciuri StatsBomb, doar pentru
  **sezoanele aproape complete** (de ex. Premier League 2015/16, La Liga 2015/16, Euro 2020,
  Cupele Mondiale). Sezoanele parțiale (de ex. doar meciurile Barcelonei) sunt excluse,
  pentru că ar da clasamente greșite.
- Șuturile, xG-ul și statisticile jucătorilor vin din evenimente: toate cele 13 meciuri ale
  Italiei și Angliei la Euro 2020.
- **Loviturile de departajare (penalty shoot-out) sunt excluse**, ca în statisticile
  oficiale. Verificare: Harry Kane are 4 goluri la Euro 2020, exact ca în realitate.

**Rezultat verificat:** `make replay-many` a trimis 3 meciuri în paralel la 60x (12.051
evenimente în 151 de secunde, 0 erori), iar testul a dat PASS pentru toate trei, inclusiv
pentru meciul cu prelungiri.

## 6. Faza 4: pagina de tendințe și pregătirea demo-ului

**Ce am făcut**
- Pagina 3, **League trends**: selector de ligi; goluri pe meci pe sezon, goluri acasă vs
  deplasare, procent victorii acasă / egaluri / victorii în deplasare. Exemplu: în marile
  ligi 2015/16 sunt 2,63 goluri pe meci, 44% victorii acasă și 29% în deplasare, adică
  avantajul terenului propriu se vede în date.
- `scripts/healthcheck.sh` (`make health`): verifică fiecare serviciu din docker-compose și
  afișează OK/FAIL. Recunoaște automat și serviciile pe care le vor adăuga Mihai și Dayana
  (Redis, Grafana, Spark).
- `docs/demo-script.md`: scenariul de demo de ~10 minute, cu comenzile, ce arăți pe fiecare
  ecran, rezultatul așteptat și ce faci dacă un pas nu merge.

**Rezultat verificat:** `make health` → „All checks passed"; toate cele 4 pagini Streamlit
rulează fără erori, testate și în interiorul containerului.

---

## 7. Probleme întâlnite și cum le-am rezolvat

| Problemă | Soluție |
|---|---|
| Timestamp-uri greșite în datele StatsBomb | sortare după `index`; pauze calculate față de timpul maxim (vezi Faza 2) |
| Imaginile oficiale MinIO au fost retrase în 2025 (nu mai există pe Docker Hub) | folosim `pgsty/minio`, același server MinIO, construit de comunitate, pentru arm64 și amd64 |
| Folderul `Documents` este sincronizat cu iCloud; Mac-ul înlocuiește fișierele cu „placeholder-e", iar Docker nu le poate citi (`Errno 35`) | scripturile descarcă automat fișierele înainte de rulare; recomandare: mutați proiectul în afara `Documents` sau setați `DATA_DIR_HOST` |
| Python de la python.org pe Mac nu are certificate SSL | `make fetch-data` rulează descărcarea în Docker |
| Testul putea verifica un replay vechi în loc de cel nou | `--wait` ignoră replay-urile terminate înainte de pornirea testului |

## 8. Pașii următori

**David**
1. Repetă demo-ul după `docs/demo-script.md` și fă capturi de ecran ca rezervă.
2. Când Mihai livrează tabelele gold: rulează Streamlit pe ele (bannerul „DEV DATA" dispare
   automat) și verifică dacă coloanele corespund contractului.
3. Recomandat: mută repo-ul în afara folderului `Documents` (de ex. `~/dev/match-pulse`), ca
   iCloud să nu mai interfereze cu Docker.
4. Urcă proiectul pe GitHub (`git push`). Toate commit-urile sunt făcute local.

**Mihai**
1. Să citească și să aprobe `docs/gold-contract.md` (are 6 întrebări deschise la final).
2. Să preia serviciul `minio` din docker-compose și să adauge Spark batch.
3. Să creeze `batch/load_history.sh` și `batch/run_batch.sh` (apelate de `make load-history` /
   `make run-batch`) și să scrie tabelele în `s3://lake/gold/<tabel>`.
   `dev_fixtures.py` poate servi ca exemplu de calcul.

**Dayana**
1. Să citească `docs/event-schema.md`: formatul mesajelor, ordinea garantată per meci și
   faptul că `clock_seconds` poate merge rar înapoi (timestamp-uri greșite în sursă).
2. Să consume `match-events` și `match-control` de la `kafka:9092` și să scrie în `match-alerts`.
3. Să adauge Redis, Spark Streaming și Grafana în secțiunea ei din docker-compose;
   `make health` le va verifica automat.

**Echipa**
- Să rulați împreună `make up && make health` pe fiecare laptop (Mac, Windows/WSL), ca să
  confirmați că totul pornește peste tot.

---

## 9. Istoricul commit-urilor (local, nepublicat)

```
fe23a1f Faza 4: pagina League trends, healthcheck, scenariul de demo
ea5245a Faza 3: replay paralel, Makefile + scripturi, aplicația Streamlit
228810f Script de descărcare a datelor StatsBomb
d40885b Faza 2: replay producer, test consumer, teste unitare
7776064 Faza 1: contractele (schema mesajelor Kafka, tabelele gold)
8b0c9b5 Faza 1: Kafka (KRaft), Kafka UI, crearea topic-urilor
0d888db Faza 1: structura repository-ului
```
