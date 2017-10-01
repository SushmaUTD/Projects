package pro2;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";

	public static void main(String[] args){}
	public static short calci_payld(String[] values, String[] dataType){
		int no = 1 + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++){
			String dt = dataType[i];
			switch(dt){
				case "TINYINT":
					no = no + 1;
					break;
				case "SMALLINT":
					no = no + 2;
					break;
				case "INT":
					no = no + 4;
					break;
				case "BIGINT":
					no = no + 8;
					break;
				case "REAL":
					no = no + 4;
					break;		
				case "DOUBLE":
					no = no + 8;
					break;
				case "DATETIME":
					no = no + 8;
					break;
				case "DATE":
					no = no + 8;
					break;
				case "TEXT":
					String text = values[i];
					int len = text.length();
					no = no + len;
					break;
				default:
					break;
			}
		}
		return (short)no;
	}
	public static int build_pg_inside(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x05);
		}catch(Exception e){
			System.out.println("There is a fault at interior page");
		}

		return num_pages;
	}
	public static int pg_inside(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x0D);
		}catch(Exception e){
			System.out.println("There is an error at branch page");
		}

		return num_pages;

	}
	public static int center_key_search(RandomAccessFile file, int page){
		int num = 0;
		try{
			file.seek((page-1)*pageSize);
			byte pg_field = file.readByte();
			int cell_no = retrieve_cellno(file, page);
			int center = (int) Math.ceil((double) cell_no / 2);
			long location = goffset_obtain(file, page, center-1);
			file.seek(location);

			switch(pg_field){
				case 0x05:
					num = file.readInt(); 
					num = file.readInt();
					break;
				case 0x0D:
					num = file.readShort();
					num = file.readInt();
					break;
			}

		}catch(Exception e){
			System.out.println("there is an error at finding center key");
		}

		return num;
	}
	public static void pg_divide(RandomAccessFile file, int prestpg, int newPage){
		try{
			int cell_no = retrieve_cellno(file, prestpg);
			int center = (int) Math.ceil((double) cell_no / 2);

			int na = center - 1;
			int nb = cell_no - na;
			int content = 512;
for(int i = na; i < cell_no; i++){
				long loc = goffset_obtain(file, prestpg, i);
				file.seek(loc);
				int cellSize = file.readShort()+6;
				content = content - cellSize;
				file.seek(loc);
 				byte[] cell = new byte[cellSize];
				file.read(cell);
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				offset_modify(file, newPage, i - na, content);
			}
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);
			short offset = offset_obtain(file, prestpg, na-1);
			file.seek((prestpg-1)*pageSize+2);
			file.writeShort(offset);
			int rightMost = retrieve_right(file, prestpg);
			modify_right(file, newPage, rightMost);
			modify_right(file, prestpg, newPage);
			int parent = retrieve_prev(file, prestpg);
			modify_prev(file, newPage, parent);
			byte num = (byte) na;
			modify_cellno(file, prestpg, num);
			num = (byte) nb;
			modify_cellno(file, newPage, num);
		}catch(Exception e){
			System.out.println("There is a fault at page");
			e.printStackTrace();
		}
	}
	public static void divide_page_inside(RandomAccessFile file, int curPage, int newPage){
		try{
			int cell_no = retrieve_cellno(file, curPage);
			int center = (int) Math.ceil((double) cell_no / 2);

			int na = center - 1;
			int nb = cell_no - na - 1;
			short content = 512;

			for(int i = na+1; i < cell_no; i++){
				long loc = goffset_obtain(file, curPage, i);
				short sz_cl = 8;
				content = (short)(content - sz_cl);
				file.seek(loc);
				byte[] cell = new byte[sz_cl];
				file.read(cell);
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				file.seek(loc);
				int page = file.readInt();
				modify_prev(file, page, newPage);
				offset_modify(file, newPage, i - (na + 1), content);
			}
			int tmp = retrieve_right(file, curPage);
			modify_right(file, newPage, tmp);
			long midLoc = goffset_obtain(file, curPage, center - 1);
			file.seek(midLoc);
			tmp = file.readInt();
			modify_right(file, curPage, tmp);
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);
			short offset = offset_obtain(file, curPage, na-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(offset);
			int parent = retrieve_prev(file, curPage);
			modify_prev(file, newPage, parent);
			byte num = (byte) na;
			modify_cellno(file, curPage, num);
			num = (byte) nb;
			modify_cellno(file, newPage, num);
		}catch(Exception e){
			System.out.println("There is anerror at dividing the leaf page");
		}
	}
	public static void divide_branch(RandomAccessFile file, int page){
		int newPage = pg_inside(file);
		int midKey = center_key_search(file, page);
		pg_divide(file, page, newPage);
		int parent = retrieve_prev(file, page);
		if(parent == 0){
			int rootPage = build_pg_inside(file);
			modify_prev(file, page, rootPage);
			modify_prev(file, newPage, rootPage);
			modify_right(file, rootPage, newPage);
			cell_add_inside(file, rootPage, page, midKey);
		}else{
			long ploc = retrieve_ptr_loc(file, page, parent);
			modify_ptr_loc(file, ploc, parent, newPage);
			cell_add_inside(file, parent, page, midKey);
			order_array(file, parent);
			while(verify_space_inside(file, parent)){
				parent = divide_inside(file, parent);
			}
		}
	}
	public static int divide_inside(RandomAccessFile file, int page){
		int n_pg = build_pg_inside(file);
		int center_ky = center_key_search(file, page);
		divide_page_inside(file, page, n_pg);
		int prev = retrieve_prev(file, page);
		if(prev == 0){
			int rt_pg = build_pg_inside(file);
			modify_prev(file, page, rt_pg);
			modify_prev(file, n_pg, rt_pg);
			modify_right(file, rt_pg, n_pg);
			cell_add_inside(file, rt_pg, page, center_ky);
			return rt_pg;
		}else{
			long ploc = retrieve_ptr_loc(file, page, prev);
			modify_ptr_loc(file, ploc, prev, n_pg);
			cell_add_inside(file, prev, page, center_ky);
			order_array(file, prev);
			return prev;
		}
	}

	public static void order_array(RandomAccessFile file, int page){
		 byte num = retrieve_cellno(file, page);
		 int[] arr_k = obtain_key_arr(file, page);
		 short[] arr_c = retrieve_cellarray(file, page);
		 int ltmp;
		 short rtmp;

		 for (int i = 1; i < num; i++) {
            for(int j = i ; j > 0 ; j--){
                if(arr_k[j] < arr_k[j-1]){

                    ltmp = arr_k[j];
                    arr_k[j] = arr_k[j-1];
                    arr_k[j-1] = ltmp;

                    rtmp = arr_c[j];
                    arr_c[j] = arr_c[j-1];
                    arr_c[j-1] = rtmp;
                }
            }
         }

         try{
         	file.seek((page-1)*pageSize+12);
         	for(int i = 0; i < num; i++){
				file.writeShort(arr_c[i]);
			}
         }catch(Exception e){
         	System.out.println("there is an error at ordering the cell array");
         }
	}

	public static int[] obtain_key_arr(RandomAccessFile file, int page){
		int n = new Integer(retrieve_cellno(file, page));
		int[] array = new int[n];

		try{
			file.seek((page-1)*pageSize);
			byte pageType = file.readByte();
			byte offset = 0;
			switch(pageType){
				case 0x05:
					offset = 4;
					break;
				case 0x0d:
					offset = 2;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < n; i++){
				long loc = goffset_obtain(file, page, i);
				file.seek(loc+offset);
				array[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println("There is an retieving the cell key array");
		}

		return array;
	}

	public static short[] retrieve_cellarray(RandomAccessFile file, int page){
		int num = new Integer(retrieve_cellno(file, page));
		short[] arr = new short[num];

		try{
			file.seek((page-1)*pageSize+12);
			for(int i = 0; i < num; i++){
				arr[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println("There is an error at retrieving the cell array");
		}

		return arr;
	}
	public static int retrieve_prev(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize+8);
			val = file.readInt();
		}catch(Exception e){
			System.out.println("There is an error at retieving the parent");
		}

		return val;
	}

	public static void modify_prev(RandomAccessFile file, int page, int parent){
		try{
			file.seek((page-1)*pageSize+8);
			file.writeInt(parent);
		}catch(Exception e){
			System.out.println("There is an error at previous");
		}
	}
	public static long retrieve_ptr_loc(RandomAccessFile file, int page, int parent){
		long val = 0;
		try{
			int c_no = new Integer(retrieve_cellno(file, parent));
			for(int i=0; i < c_no; i++){
				long loc = goffset_obtain(file, parent, i);
				file.seek(loc);
				int childPage = file.readInt();
				if(childPage == page){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println("There is an error at retrievig the pointer loc");
		}

		return val;
	}
	public static void modify_ptr_loc(RandomAccessFile file, long location, int prev, int page){
		try{
			if(location == 0){
				file.seek((prev-1)*pageSize+4);
			}else{
				file.seek(location);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println("there is an error at pointer location");
		}
	} 
	public static void cell_add_inside(RandomAccessFile file, int page, int child, int key){
		try{
			file.seek((page-1)*pageSize+2);
			short content = file.readShort();
			if(content == 0)
				content = 512;
			content = (short)(content - 8);
			file.seek((page-1)*pageSize+content);
			file.writeInt(child);
			file.writeInt(key);
			file.seek((page-1)*pageSize+2);
			file.writeShort(content);
			byte num = retrieve_cellno(file, page);
			offset_modify(file, page ,num, content);
			num = (byte) (num + 1);
			modify_cellno(file, page, num);

		}catch(Exception e){
			System.out.println("There is an error at inside cell");
		}
	}
	public static void cell_add(RandomAccessFile file, int page, int offset, short plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			file.writeByte(col);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
			int n = retrieve_cellno(file, page);
			byte tmp = (byte) (n+1);
			modify_cellno(file, page, tmp);
			file.seek((page-1)*pageSize+12+n*2);
			file.writeShort(offset);
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*pageSize+2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println("There is an error at adding the cell");
			e.printStackTrace();
		}
	}
	
	
	public static void cell_drop(RandomAccessFile file, int page, int offset, short plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			file.writeByte(col);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				vals[i]=null;
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
			int n = retrieve_cellno(file, page);
			byte tmp = (byte) (n+1);
			modify_cellno(file, page, tmp);
			file.seek((page-1)*pageSize+12+n*2);
			file.writeShort(offset);
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*pageSize+2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println("There is an error at leaf cell");
			e.printStackTrace();
		}
	}

	public static void modify_lf_cel(RandomAccessFile file, int page, int offset, int plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			file.writeByte(col);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println("There is an error at updation of page");
			System.out.println(e);
		}
	}

	public static int retrieve_right(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize+4);
			val = file.readInt();
		}catch(Exception e){
			System.out.println("There is an error at updation at rightmost");
		}

		return val;
	}

	public static void modify_right(RandomAccessFile file, int page, int rightMost){

		try{
			file.seek((page-1)*pageSize+4);
			file.writeInt(rightMost);
		}catch(Exception e){
			System.out.println("Error at setRightMost");
		}

	}
	public static byte retrieve_cellno(RandomAccessFile file, int page){
		byte val = 0;

		try{
			file.seek((page-1)*pageSize+1);
			val = file.readByte();
		}catch(Exception e){
			System.out.println(e);
			System.out.println("Error at getCellNumber");
		}

		return val;
	}

	public static void modify_cellno(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*pageSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println("there is an error at modifying the leaf space");
		}
	}
	public static boolean verify_space_inside(RandomAccessFile file, int page){
		byte numCells = retrieve_cellno(file, page);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	// 
	public static int verify_space_branch(RandomAccessFile file, int page, int size){
		int val = -1;

		try{
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content == 0)
				return pageSize - size;
			int numCells = retrieve_cellno(file, page);
			int space = content - 20 - 2*numCells;
			if(size < space)
				return content - size;
			
		}catch(Exception e){
			System.out.println("There is an error at verifying the leaf space inside");
		}

		return val;
	}
	public static boolean key_present(RandomAccessFile file, int page, int key){
		int[] ar = obtain_key_arr(file, page);
		for(int i : ar)
			if(key == i)
				return true;
		return false;
	}
	public static long goffset_obtain(RandomAccessFile file, int page, int id){
		long location = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			short offset = file.readShort();
			long begin = (page-1)*pageSize;
			location = begin + offset;
		}catch(Exception e){
			System.out.println("there is an error at retieving the location of cell");
		}
		return location;
	}

	public static short offset_obtain(RandomAccessFile file, int page, int id){
		short offset = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			offset = file.readShort();
		}catch(Exception e){
			System.out.println("There is a fault at retrieving cell offset");
		}
		return offset;
	}

	public static void offset_modify(RandomAccessFile file, int page, int id, int offset){
		try{
			file.seek((page-1)*pageSize+12+id*2);
			file.writeShort(offset);
		}catch(Exception e){
			System.out.println("There is a fault at cell offset");
		}
	}
}















