# Content-Based Publish-Subscribe System

Acest proiect implementeaza un sistem distribuit de tip Publish-Subscribe bazat pe continut (Content-Based Routing). Sistemul permite rutarea eficienta a publicatiilor (evenimente bursiere) catre clientii interesati, evaluand reguli complexe de filtrare intr-un mediu descentralizat, securizat si tolerant la defecte.

## Arhitectura Sistemului si Fluxul Aplicatiei

Topologia retelei este formata din trei componente principale: nodurile Publisher (2 instante paralele), nodurile Broker (3 instante ce formeaza un overlay network) si nodurile Subscriber (clientii).

### 1. Fluxul de Publicare (Publisher)
Cele 2 noduri Publisher functioneaza ca generatoare independente de trafic. 
* Fiecare Publisher genereaza la intervale regulate publicatii ce contin date financiare (numele companiei, valoarea actiunii, scaderea, variatia si data).
* Inainte de a parasi nodul, datele sunt criptate pe loc (Zero-Knowledge) pentru a ascunde semantica fata de Brokeri.
* Publisher-ul alege aleatoriu unul dintre cei 3 Brokeri (Load Balancing nativ) si deschide o conexiune TCP scurta.
* Publicatia este serializata in format binar folosind Google Protocol Buffers si transmisa catre Broker. Odata trimis, socket-ul este inchis (model fire-and-forget).

### 2. Fluxul de Subscriere (Subscriber)
Nodurile Subscriber sunt clientii finali care doresc sa primeasca notificari.
* Fiecare worker asociat unui subscriber isi cripteaza lista de filtre inainte de a interactiona cu reteaua.
* Se conecteaza la un port de Broker alocat si trimite intentia de abonare continand filtrele criptate, folosind mesaje text in format JSON.
* Odata abonat, Subscriber-ul intra intr-o bucla de ascultare infinita pe acel socket TCP, asteptand sa citeasca notificarile primite de la Broker.

### 3. Motorul de Rutare si Reteaua Overlay (Broker)
Cei 3 Brokeri reprezinta stratul de middleware (de tip stateless privind istoricul, stocand doar starea curenta a conexiunilor).
* **Protocol Multiplexing:** Cand un client se conecteaza, Brokerul analizeaza primul byte (octet) primit pe retea pentru a deduce protocolul. Daca este `{` (cod ASCII 123), stie ca este un mesaj JSON (de la un Subscriber sau un alt Broker). In caz contrar, stie ca este un mesaj binar Protobuf (de la un Publisher).
* **Evaluarea (Matching):** Cand o publicatie este primita, Brokerul itereaza prin dictionarul din memoria RAM unde tine conexiunile TCP (socket-urile) si filtrele Subscriberilor curenti. Executa functia de validare criptografica. Pentru fiecare potrivire, trimite instantaneu publicatia inapoi clientului, in format JSON.
* **Propagarea in retea (Forwarding):** Deoarece un Subscriber conectat la Brokerul 1 trebuie sa primeasca actiuni trimise de un Publisher catre Brokerul 2, aplicatia foloseste un mecanism de Overlay Network. Cand un Broker primeste un mesaj direct de la un Publisher, pe langa procesarea locala, el trimite publicatia mai departe catre toti ceilalti Brokeri vecini. Mesajele rutate intre Brokeri folosesc un flag special (`"forwarded": True`) pentru a asigura trasabilitatea si a preveni buclele infinite (un mesaj trimis mai departe nu va fi re-trimis de celalalt Broker).

---

## Implementarea functionalitatilor Bonus

Pe langa arhitectura de baza, proiectul a integrat trei cerinte de nivel avansat pentru securitate, performanta si rezilienta.

### Bonus 1: Mecanism de serializare binara (Google Protocol Buffers)
Pentru a optimiza latimea de banda a retelei si timpul de transmisie, fluxul de date de la nodurile Publisher catre Brokers a fost implementat folosind Google Protobuf.
* S-a definit o schema stricta a structurii de date intr-un fisier `.proto` (`publication.proto`), care a fost compilat in cod sursa Python.
* **Framing binar:** Deoarece protocoalele binare nu pot folosi caractere delimitatoare (cum este `\n` in JSON) peste TCP, s-a implementat tehnica *Length-Prefixed Framing*. Publisher-ul calculeaza lungimea pachetului binar, o impacheteaza in exact 4 bytes (Big-Endian) si o pune ca prefix. La randul sau, Brokerul citeste exact acesti 4 bytes pentru a afla cat de mare este mesajul care urmeaza, prevenind citirile incomplete sau corupte (buffer overflow/underflow).

### Bonus 2: Toleranta la defecte (Failover pe nodurile Broker)
Sistemul asigura livrarea continua a notificarilor chiar daca un nod Broker este inchis fortat sau sufera un crash, fara pierderi de date.
* Monitorizarea conexiunilor TCP este realizata activ la nivelul Subscriberului. Metoda de citire asincrona (`reader.readline()`) actioneaza ca un senzor.
* Cand un Broker se prabuseste, senzorul Subscriberului returneaza instantaneu un pachet gol (EOF - End of File). Aceasta intrerupere este prinsa in cod, iar executia paraseste ramura curenta si declanseaza logica de fallback.
* Subscriber-ul extrage dinamic portul cazut din lista de porturi disponibile, alege un alt Broker valid si initiaza o conexiune TCP noua, trimitand din nou lista de filtre criptate. Astfel, abonamentele sunt migrate si relocate automat catre nodurile sanatoase ale sistemului in cateva secunde.

### Bonus 3: Filtrarea mesajelor in regim Zero-Knowledge (Criptare)
Pentru a garanta confidentialitatea datelor in tranzit si pe servere, Brokerii realizeaza actiunea de content-matching exclusiv pe date criptate.
* O publicatie sau o inregistrare de tip filtru este criptata de emitent inainte de a ajunge la middleware, iar Brokerii nu detin chei de decriptare.
* **Egalitati (String-uri):** Numele companiilor sunt criptate folosind Deterministic Hashing (ex. SHA-256). Astfel, Brokerul compara direct amprentele hash (`Hash(pub_val) == Hash(sub_val)`) fara a sti vreodata numele real al companiei.
* **Inegalitati (Numere/Date):** Deoarece operatorii de inegalitate (`<, >, <=, >=`) sunt esentiali in domeniul bursier, s-a utilizat un algoritm de tip Order-Preserving Encryption (OPE). Valorile numerice si datele calendaristice sunt trecute printr-o functie de criptare monotona (ex. o ecuatie liniara constanta). Aceasta mascheaza valoarea exacta in timpul tranzitului, dar conserva ordinea si ierarhia matematica, permitand motorului static de matching sa aplice inegalitati corecte si precise, respectand regulile criptografice.