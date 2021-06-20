package com.distributedsystems.pubsubsystem;

import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Context;
import android.content.ContextWrapper;
import android.content.Intent;
import android.graphics.PixelFormat;
import android.media.MediaPlayer;
import android.net.Uri;
import android.os.Build;
import android.os.Environment;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.MediaController;
import android.os.Bundle;
import android.widget.VideoView;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;

public class videoActivity extends AppCompatActivity {

    MediaController ctrl;
    VideoView videoView = null;
    Button playBtn;
    ImageButton returnBtn;
    String videoName;
    String video;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setFormat(PixelFormat.TRANSLUCENT);
        setContentView(R.layout.activity_video);
        returnBtn = findViewById(R.id.returnBtn_VideoAct);
        videoView = findViewById(R.id.videoView);
        playBtn = findViewById(R.id.playBtn);
        ctrl = new MediaController(this);
        videoView.setMediaController(ctrl);
        videoView.requestFocus();
        ContextWrapper contextWrapper = new ContextWrapper(getApplicationContext());
        File dir = contextWrapper.getDir(getFilesDir().getName(), Context.MODE_PRIVATE);
        videoName = getIntent().getStringExtra("name");
        video = dir.toString()+"/"+ videoName;
        playBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                if(playBtn.getText().equals("PLAY")){
                    Uri uri = Uri.parse(video);
                    videoView.setVideoURI(uri);
                    videoView.start();
                }else{
                    videoView.pause();
                }
                changeText();
            }
        });
        returnBtn.setOnClickListener(new View.OnClickListener() {
            @RequiresApi(api = Build.VERSION_CODES.P)
            @Override
            public void onClick(View v) {
                cleanupVideo();
                Intent results = new Intent();
                results.putExtra("name", videoName);
                setResult(Activity.RESULT_OK, results);
                finish();
            }
        });
    }

    private void changeText(){
        if(playBtn.getText().equals("PLAY")){
            playBtn.setText("PAUSE");
        }else{
            playBtn.setText("PLAY");
        }
    }

    private void cleanupVideo(){
        if(videoView.getVisibility() == View.VISIBLE){
            videoView.stopPlayback();
            videoView.clearAnimation();
            videoView.suspend();
            videoView.setVideoURI(null);
            videoView = null;
        }
    }
}