package com.distributedsystems.pubsubsystem;

import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.ListView;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class resultsActivity extends AppCompatActivity {

    TextView topicText;
    Button subBtn;
    ImageButton returnBtn;
    String[] data;
    boolean was_subscribed;
    boolean is_subscribed;
    String topic;
    ArrayList<feedActivity.VideoString> videos = new ArrayList<>();
    ListView listView;
    ArrayAdapter<feedActivity.VideoString> adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_results);
        ArrayList<String> raw_videos;
        topicText = findViewById(R.id.topicText);
        subBtn = findViewById(R.id.subBtn);
        listView = findViewById(R.id.resultsList);
        returnBtn = findViewById(R.id.returnBtn);
        data = getIntent().getStringArrayExtra("data");
        topic = data[0];
        is_subscribed = was_subscribed = Boolean.parseBoolean(data[1]);
        raw_videos = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(data, 2, data.length)));
        topicText.setText(topic);
        if(is_subscribed){
            subBtn.setText("UNSUBSCRIBE");
            subBtn.setBackgroundColor(Color.WHITE);
            subBtn.setTextColor(Color.BLACK);
        }else{
            subBtn.setText("SUBSCRIBE");
            subBtn.setTextColor(Color.parseColor("#FFFFFF"));
            subBtn.setBackgroundColor(Color.parseColor("#800080"));
        }
        for(String str : raw_videos){
            String[] data = str.split("\\|");
            videos.add(new feedActivity.VideoString(data[0].split("=")[1], data[1].split("=")[1],Float.parseFloat(data[2].split("=")[1]), data[3].split("=")[1], str));
        }
        Collections.sort(videos);
    }

    @Override
    protected void onStart() {
        super.onStart();
        adapter = new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, videos);
        listView.setAdapter(adapter);
        subBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                changeButtonState();
            }
        });

        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Intent results = new Intent();
                results.putExtra("video", videos.get(position).getOriginal());
                boolean changed = false;
                if(subBtn.getText().equals("SUBSCRIBE"))
                    is_subscribed = false;
                else
                    is_subscribed = true;
                if(was_subscribed != is_subscribed)
                    changed = true;
                results.putExtra("changed", changed);
                results.putExtra("subscribe", is_subscribed == true);
                results.putExtra("topic", topic);
                setResult(Activity.RESULT_OK, results);
                finish();
            }
        });
        returnBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent results = new Intent();
                boolean changed = false;
                if(subBtn.getText().equals("SUBSCRIBE"))
                    is_subscribed = false;
                else
                    is_subscribed = true;
                if(was_subscribed != is_subscribed)
                    changed = true;
                results.putExtra("changed", changed);
                results.putExtra("subscribe", is_subscribed == true);
                results.putExtra("topic", topic);
                setResult(Activity.RESULT_CANCELED, results);
                finish();
            }
        });
    }

    private void changeButtonState(){
        if(is_subscribed){
            subBtn.setText("SUBSCRIBE");
            subBtn.setBackgroundColor(Color.parseColor("#800080"));
            subBtn.setTextColor(Color.parseColor("#FFFFFF"));
        }else{
            subBtn.setText("UNSUBSCRIBE");
            subBtn.setBackgroundColor(Color.WHITE);
            subBtn.setTextColor(Color.BLACK);
        }
        is_subscribed = !is_subscribed;
    }


}