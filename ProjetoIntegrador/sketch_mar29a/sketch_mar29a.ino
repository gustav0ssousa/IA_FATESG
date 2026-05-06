// Código Simples para Piscar LED no ESP32
const int pinoLED = 2; // GPIO2 é frequentemente o LED interno (blue LED)

void setup() {
  pinMode(pinoLED, OUTPUT); // Define o pino do LED como saída
}

void loop() {
  digitalWrite(pinoLED, HIGH); // Liga o LED
  delay(1000);                // Espera 1 segundo (1000 milissegundos)
  digitalWrite(pinoLED, LOW);  // Desliga o LED
  delay(1000);                // Espera 1 segundo
}
