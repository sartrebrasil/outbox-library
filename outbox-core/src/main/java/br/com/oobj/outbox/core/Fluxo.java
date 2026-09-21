package br.com.oobj.outbox.core;

/**
 * Unidade de reivindicação, ordenação e bloqueio (02 §4.2): o par agregado x destino.
 */
record Fluxo(String aggregateId, String destination) {
}
