/**
 * 
 */
package com.android.googlephots;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.google.gdata.client.photos.PicasawebService;
import com.google.gdata.data.PlainTextConstruct;
import com.google.gdata.data.media.MediaFileSource;
import com.google.gdata.data.photos.PhotoEntry;
import com.google.gdata.util.ServiceException;


/**
 * @author Sushma
 *
 */
@Controller
public class UploadPhotos {
	
	@RequestMapping(value = "/uploadFile", method = RequestMethod.POST)
	public String uploadToGooglePhots(@RequestParam("files") MultipartFile[] files) throws MalformedURLException, ServiceException
	{
		PicasawebService myService = new PicasawebService("Upload Photos to Google Photos app");
		myService.setUserCredentials("sush.orange@gmail.com", "Sushmasush30_");
		URL albumPostUrl = new URL("https://picasaweb.google.com/data/feed/api/user/sushma/profile-photos/103074890959648537031/");
		for (MultipartFile file : files) {
	        if (file.isEmpty()) {
	            continue;
	        }
	        try {
	            PhotoEntry myPhoto = new PhotoEntry();
	    		myPhoto.setTitle(new PlainTextConstruct("Test Photos"));
	    		myPhoto.setDescription(new PlainTextConstruct("Photos taken using Android App."));
	    		myPhoto.setClient("sushma");
	    		MediaFileSource myMedia = new MediaFileSource((File) file, "image/jpeg");
	    		myPhoto.setMediaSource(myMedia);
	    		PhotoEntry returnedPhoto = myService.insert(albumPostUrl, myPhoto);
	        } catch (IOException e) {
	            e.printStackTrace();
	            return "Error while uploading Photos";
	        }
	    }
		return "Uploaded Successfully";
	}
}
