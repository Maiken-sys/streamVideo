package com.distributedsystems.pubsubsystem;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.util.Log;
import android.util.Pair;
import android.view.View;
import android.widget.Adapter;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.ListView;
import android.widget.Toast;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class feedActivity extends AppCompatActivity {

    ListView listView;
    EditText searchText;
    ImageButton searchBtn;
    ArrayAdapter<VideoString> adapter;
    Publisher publisher;
    Consumer consumer;
    String username;
    String password;
    Handler feedHandler;
    int interval = 5000;
    LinkedBlockingQueue<String> videos = new LinkedBlockingQueue<>();
    ArrayList<VideoString> videoList = new ArrayList<>();


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        feedHandler = new Handler();
        setContentView(R.layout.activity_feed);
        listView = findViewById(R.id.feedList);
        searchText = findViewById(R.id.searchText);
        searchBtn = findViewById(R.id.imageButton);
        username = getIntent().getStringExtra("username");
        password = getIntent().getStringExtra("password");
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        stopRepeatingChecks();
    }

    @Override
    protected void onStart() {
        super.onStart();
        connect(username, password);
        new Listener().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        Object[] v = videos.toArray();
        for(Object obj : v){
            String video_str = obj.toString();
            String[] data = video_str.split("\\|");
            videoList.add(new VideoString(data[0].split("=")[1], data[1].split("=")[1],Float.parseFloat(data[2].split("=")[1]), (data[3].split("=")[1])));

        }
        adapter = new ArrayAdapter<VideoString>(this, android.R.layout.simple_list_item_1, videoList);
        listView.setAdapter(adapter);
        startRepeatingChecks();
        searchBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                request(searchText.getText().toString());
            }
        });
    }

    public boolean connect(String username, String password) {
        publisher = new Publisher(username);
        consumer = new Consumer(username);
        try{
            new Connection().execute(new Pair<>(username, password)).get();
            return true;
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
        return false;
    }

    private void request(String req){
        new Request().execute(req);
        long time_requested = System.currentTimeMillis();
        while(consumer.getRequest_data().isEmpty() || Math.abs(time_requested - System.currentTimeMillis()) > 4000){

        }
    }

    private class Connection extends AsyncTask<Pair<String, String>, String, String> {


        String username;
        String password;

        @Override
        protected String doInBackground(Pair<String, String>... pairs) {
            this.username = pairs[0].first;
            this.password = pairs[0].second;
            publisher.connect();
            publisher.initialize_structures();
            consumer.connect();
            consumer.sendInfo();
            consumer.initialize_connections();
            return null;
        }
    }

    private class Request extends AsyncTask<String, String, String[]>{

        String[] result;
        @Override
        protected String[] doInBackground(String... strings) {
            result = consumer.request(strings[0]);
            return result;
        }
    }

    private class Listener extends AsyncTask<Void, Void, Void>{


        ArrayList<VideoString> arrayList = new ArrayList<>();
        @Override
        protected Void doInBackground(Void... voids) {
            Object[] videos_received = consumer.getVideos().toArray();
            for(Object obj : videos_received){
                String[] data = obj.toString().split("\\|");
                arrayList.add(new VideoString(data[0].split("=")[1], data[1].split("=")[1],Float.parseFloat(data[2].split("=")[1]), data[3].split("=")[1]));
            }
            return null;
        }

        @Override
        protected void onPostExecute(Void unused) {
            super.onPostExecute(unused);
            videoList.clear();
            for(VideoString vs : arrayList){
                videoList.add(vs);
            }
            Collections.sort(videoList);
            adapter.notifyDataSetChanged();
        }
    }

    Runnable feedChecker = new Runnable() {
        @Override
        public void run() {
            try{
                new Listener().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
            }finally {
                feedHandler.postDelayed(feedChecker, interval);
            }
        }
    };

    void startRepeatingChecks(){
        feedChecker.run();
    }

    void stopRepeatingChecks(){
        feedHandler.removeCallbacks(feedChecker);
    }

    private class VideoString implements Comparable<VideoString>{

        private String channel;
        private String videoName;
        private float duration;
        private Date dateUploaded;
        DateFormat df = new SimpleDateFormat("EEE-MMM-d-yyyy HH:mm:ss");
        public VideoString(String channel, String videoName, float duration, String dateUploaded) {
            this.channel = channel;
            this.videoName = videoName;
            this.duration = duration;
            String[] temp = dateUploaded.split(" ");
            String dd = temp[0]+"-"+temp[1]+"-"+temp[2]+"-"+temp[5]+" "+temp[3];
            try {
                this.dateUploaded = df.parse(dd);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        public String getChannel() {
            return channel;
        }

        public float getDuration() {
            return duration;
        }

        public Date getDateUploaded(){
            return dateUploaded;
        }

        @Override
        public int compareTo(VideoString o) {
            return this.dateUploaded.before(o.getDateUploaded()) == false ? -1 : 1;
        }

        @Override
        public String toString(){
            return this.videoName+"    uploaded by "+this.channel+"\n"+this.dateUploaded;
        }
    }
}