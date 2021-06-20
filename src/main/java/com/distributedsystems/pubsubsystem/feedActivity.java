package com.distributedsystems.pubsubsystem;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Context;
import android.content.ContextWrapper;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Debug;
import android.os.Handler;
import android.util.Log;
import android.util.Pair;
import android.view.View;
import android.widget.Adapter;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.ListView;
import android.widget.MediaController;
import android.widget.Toast;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
    boolean logged = false;
    int interval = 5000;
    LinkedBlockingQueue<String> videos = new LinkedBlockingQueue<>();
    ArrayList<VideoString> videoList = new ArrayList<>();


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_feed);
        feedHandler = new Handler();
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
            videoList.add(new VideoString(data[0].split("=")[1], data[1].split("=")[1],Float.parseFloat(data[2].split("=")[1]), (data[3].split("=")[1]), video_str));
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

        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                consumer.interrupt();
                receive_video(videoList.get(position).getOriginal());
            }
        });
    }

    public boolean connect(String username, String password) {
        logged = true;
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
        String[] data = new String[0];
        try {
            consumer.interrupt();
            data = new Request(req, -1).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR).get();
        }catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        if(data.length == 0){
            Toast.makeText(this, "No results found", Toast.LENGTH_LONG).show();
        }else{
            Intent results = new Intent(this, resultsActivity.class);
            results.putExtra("data", data);
            startActivityForResult(results, 1);
        }
    }

    private void receive_video(String s){

        try{
            VideoFile videoFile = new ReceiveVideo(s).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR).get();
            playVideo(videoFile);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void playVideo(VideoFile videoFile){
        Intent intent = new Intent(this, videoActivity.class);
        writeToInternalStorage(videoFile);
        intent.putExtra("name", videoFile.getChannelName()+videoFile.getVideoName());
        startActivityForResult(intent, 2);
    }

    private boolean deleteInternalStorage(String name){
        ContextWrapper contextWrapper = new ContextWrapper(getApplicationContext());
        File dir = contextWrapper.getDir(getFilesDir().getName(), Context.MODE_PRIVATE);
        File file = new File(dir, name);
        return file.delete();
    }

    private void writeToInternalStorage(VideoFile videoFile){
        ContextWrapper contextWrapper = new ContextWrapper(getApplicationContext());
        File directory = contextWrapper.getDir(getFilesDir().getName(), Context.MODE_PRIVATE);
        File file =  new File(directory, videoFile.getChannelName()+videoFile.getVideoName());
        try {
            FileOutputStream out = new FileOutputStream(file, false);
            out.write(videoFile.getVideoFileChunk());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        if(requestCode == 1){
            boolean changed = false;
            if(resultCode == Activity.RESULT_OK){
                String video = data.getStringExtra("video");
                changed = data.getBooleanExtra("changed", false);
                receive_video(video);
            }
            if (resultCode == Activity.RESULT_CANCELED) {
                changed = data.getBooleanExtra("changed", false);
            }
            if(changed == true){
                boolean subscribe = data.getBooleanExtra("subscribe", false);
                String topic = data.getStringExtra("topic");
                    try{
                    if(subscribe){
                        new Request(topic, 1).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR).get();
                    }else{
                        new Request(topic, 2).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR).get();
                    }
                    }catch (ExecutionException | InterruptedException e){
                        e.printStackTrace();
                    }

            }
        }else if(requestCode == 2){
            if(resultCode == Activity.RESULT_OK){
                String video = data.getStringExtra("name");
                deleteInternalStorage(video);
            }
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



    private class Request extends AsyncTask<Void, Void, String[]>{

        String[] result;
        String req;
        int action;
        public Request(String req, int action){
            this.req = req;
            this.action = action;
        }
        @Override
        protected String[] doInBackground(Void... voids) {
            result = consumer.request(req, action).first;
            return result;
        }
    }

    private class ReceiveVideo extends AsyncTask<Void, Void, VideoFile>{

        VideoFile result;
        String videoName;
        public ReceiveVideo(String videoName){
            this.videoName = videoName;
        }
        @Override
        protected VideoFile doInBackground(Void... voids) {
            result = consumer.request(videoName, 0).second;
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
                arrayList.add(new VideoString(data[0].split("=")[1], data[1].split("=")[1],Float.parseFloat(data[2].split("=")[1]), data[3].split("=")[1], obj.toString()));
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

    public static class VideoString implements Comparable<VideoString>{

        private String channel;
        private String videoName;
        private float duration;
        private Date dateUploaded;
        private String original;
        private DateFormat df = new SimpleDateFormat("EEE-MMM-d-yyyy HH:mm:ss");




        public VideoString(String channel, String videoName, float duration, String dateUploaded, String original) {
            this.channel = channel;
            this.videoName = videoName;
            this.duration = duration;
            this.original = original;
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

        public String getOriginal() {
            return original;
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
            return this.videoName+"    uploaded by "+this.channel+"     duration: "+duration+"\n"+this.dateUploaded;
        }
    }
}