package com.distributedsystems.pubsubsystem;

import android.os.AsyncTask;
import android.util.Pair;


public class AppNode extends AsyncTask<Pair<String, String>, String , String> {
    private Publisher publisher;
    private Consumer consumer;
    private String name;
    private String password;



    public AppNode(){

    }



    @Override
    protected String doInBackground(Pair<String, String>... strings) {
        this.name = strings[0].first;
        this.password = strings[0].second;
        publisher = new Publisher(name);
        consumer = new Consumer(name);
        publisher.connect();
        publisher.initialize_structures();
        consumer.connect();
        consumer.sendInfo();
        consumer.initialize_connections();

        //publisher.addVideo();
        //consumer.request(sc.nextLine());
        //publisher.deleteVideo();
        return null;
    }
}