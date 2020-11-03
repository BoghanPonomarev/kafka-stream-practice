package com.ponomarev.part3;

public class Demo {

    public static void main(String[] args) {
        new Thread(Demo::startProducer).start();
        new Thread(() -> new BankTransactionConsumer().consume()).start();
        while (true);
    }

    private static void startProducer() {
        try {
            new BankTransactionsProducer().startProducing();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
