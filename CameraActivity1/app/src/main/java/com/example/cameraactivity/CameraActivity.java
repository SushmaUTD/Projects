package com.example.cameraactivity;

import android.os.Bundle;
import android.app.Activity;
import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.view.View;
import android.provider.MediaStore;
import android.widget.Button;
import android.widget.ImageView;
import android.database.Cursor;

import java.io.File;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;



public class CameraActivity extends Activity {

	Button btnTakePhoto;
	Button selectPhoto;
	Button uploadPhoto;

	ImageView imgTakenPhoto;
	ImageView choosePhoto;

	HttpEntity resEntity;
	HttpRequest res;
	//No more than 10 files can be uploaded
	String[] filePaths = new String[10];
	Bitmap bitmap;
	private static final int CAM_REQUEST = 1313;
	private static final int GALLERY_REQUEST = 1212;
	private static final int UPLOAD_REQUEST = 1111;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_camera);

		btnTakePhoto = (Button) findViewById(R.id.button1);
		imgTakenPhoto = (ImageView) findViewById(R.id.imageview1);

		selectPhoto = (Button) findViewById(R.id.button2);
		choosePhoto = (ImageView) findViewById(R.id.imageview2);

		btnTakePhoto.setOnClickListener(new btnTakePhotoClicker());
		selectPhoto.setOnClickListener(new chooseTakePhotoClicker());

		uploadPhoto = (Button) findViewById(R.id.button3);
		uploadPhoto.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				Intent intent = new Intent(Intent.ACTION_PICK, MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
				intent.setType("image/*");
				intent.putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true);
				intent.setAction(Intent.ACTION_GET_CONTENT);
				startActivityForResult(Intent.createChooser(intent, "Select Picture"), UPLOAD_REQUEST);
			}
		});
	}
	public String getPath(Uri uri) {
		String[] projection = { MediaStore.Images.Media.DATA };
		Cursor cursor = managedQuery(uri, projection, null, null, null);
		int column_index = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATA);
		cursor.moveToFirst();
		return cursor.getString(column_index);
	}


	private void doFileUpload(Integer no_of_photos){

		File[] file = new File[10];
		FileBody[] fileBody = new FileBody[10];
		String urlString = "http://127.0.0.1:8080/uploadPhotos/uploadFile";
		try
		{
			HttpClient client = new DefaultHttpClient();
			HttpPost post = new HttpPost(urlString);
			MultipartEntity reqEntity = new MultipartEntity();
			for(int i=0;i<no_of_photos;i++)
			{
				file[i] = new File(filePaths[i]);
				fileBody[i] = new FileBody(file[i]);
				reqEntity.addPart("uploadedfile"+i, fileBody[i]);
			}
			reqEntity.addPart("user", new StringBody("User"));
			post.setEntity(reqEntity);
			HttpResponse response = client.execute(post);
			resEntity = response.getEntity();
			final String response_str = EntityUtils.toString(resEntity);
			if (resEntity != null) {
				runOnUiThread(new Runnable(){
					public void run() {
						try {
							Toast.makeText(getApplicationContext(),"Upload Complete. Check the server.", Toast.LENGTH_LONG).show();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}

	class chooseTakePhotoClicker implements Button.OnClickListener {

		@Override
		public void onClick(View v) {
			// TODO Auto-generated method stub
			Intent intent = new Intent(Intent.ACTION_PICK, MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
			intent.setType("image/*");
			intent.putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true);
			intent.setAction(Intent.ACTION_GET_CONTENT);
			startActivityForResult(Intent.createChooser(intent, "Select Picture"), GALLERY_REQUEST);
		}

	}

	class btnTakePhotoClicker implements Button.OnClickListener {
		@Override
		public void onClick(View v) {
			// TODO Auto-generated method stub
			Intent cameraintent = new Intent(android.provider.MediaStore.ACTION_IMAGE_CAPTURE);
			startActivityForResult(cameraintent, CAM_REQUEST);
		}
	}

	@Override
	protected void onActivityResult(int requestCode, int resultCode, Intent data) {
		// TODO Auto-generated method stub
		super.onActivityResult(requestCode, resultCode, data);

		if (requestCode == CAM_REQUEST) {
			Bitmap thumbnail = (Bitmap) data.getExtras().get("data");
			imgTakenPhoto.setImageBitmap(thumbnail);
		}

		if (requestCode == GALLERY_REQUEST) {
			Uri selectedImageUri = data.getData();
		//	imgTakenPhoto.setImageURI(selectedImageUri);
			int no_of_photos = data.getClipData().getItemCount();

			for(int i=0; i< no_of_photos;i++)
			{
				Uri selectedImage = data.getClipData().getItemAt(i).getUri();
				Bitmap bitmap = null;
				try {
					bitmap = MediaStore.Images.Media.getBitmap(this.getContentResolver(), selectedImage);
				} catch (Exception e) {
					e.printStackTrace();
				}
				ImageView imageView = (ImageView) findViewById(R.id.imageview2);
				imageView.setImageBitmap(bitmap);
			}
		}

		if(requestCode == UPLOAD_REQUEST)
		{
			int no_of_photos = data.getClipData().getItemCount();

			for(int i=0; i< no_of_photos;i++) {
				Uri selectedImage = data.getClipData().getItemAt(i).getUri();
				filePaths[i]=getPath(selectedImage);
			}
			doFileUpload(no_of_photos);
		}
	}
}
