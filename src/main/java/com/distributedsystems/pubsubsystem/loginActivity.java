package com.distributedsystems.pubsubsystem;

import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.os.Parcelable;
import android.util.Log;
import android.util.Pair;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;


public class loginActivity extends AppCompatActivity {


    Button loginBtn;
    EditText username;
    EditText password;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        username = (EditText)findViewById(R.id.usernameText);
        password = (EditText)findViewById(R.id.passwordText);
        loginBtn = (Button)findViewById(R.id.loginBtn);

    }



    @Override
    protected void onStart() {
        super.onStart();
        Intent intent = new Intent(this, feedActivity.class);
        loginBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                intent.putExtra("username", username.getText().toString());
                intent.putExtra("password", password.getText().toString());
                startActivity(intent);
                finish();
            }
        });
    }
}