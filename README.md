# ASVSP

Projekat iz predmeta Arhitektura sistema velikih skupova podataka

Sistemi za batch i streaming obradu pokreću se pomoću odgovarajućih docker-compose fajlova

Kao data-set je korišćen privatni skup c# fajlova, koji nisu u mom vlasništvu, pa ne mogu da ih objavim. Ovde sam postavio mali podskup od 5MB i ~1000 fajlova.

Cilj obrade bio je da se sredi source kod, iybace importi, komentari i nepotrebne prazne linije. pored toga je u sklopu streaming obrade urađena jednostavna statistika gde broji koliko često se koriste određene biblioteke.

U data-set se mogu dodati bilo koji fajlovi sa c# source kodom.

U java-project nalaze se klase, 1 producer, 2 consumera i 2 Kafka Stream-a za obradu.

![Arhitektura cela slika](https://github.com/VeljkoMaksimovic/ASVSP/edit/main/ASVSP2.png)
<img src=“ttps://github.com/VeljkoMaksimovic/ASVSP/edit/main/ASVSP2.png”>
