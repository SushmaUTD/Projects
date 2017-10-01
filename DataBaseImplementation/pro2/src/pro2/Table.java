package pro2;

import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;
	public static void main(String[] args){}
	public static void show(){
		String[] cul_mns = {"table_name"};
		String[] est = new String[0];
		String table = "davisbase_tables";
		select(table, cul_mns, est);
	}
	public static void drop(String table){
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int nm_pgs = pages(file);
			for(int pg = 1; pg <= nm_pgs; pg ++){
				file.seek((pg-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] array_unit = Page.retrieve_cellarray(file, pg);
					int i = 0;
					for(int j = 0; j < array_unit.length; j++){
						long location = Page.goffset_obtain(file, pg, j);
						String[] py_l = get_payld(file, location);
						String tl = py_l[1];
						if(!tl.equals(table)){
							Page.offset_modify(file, pg, i, array_unit[j]);
							i++;
						}
					}
					Page.modify_cellno(file, pg, (byte)i);
				}
			}
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			nm_pgs = pages(file);
			for(int page = 1; page <= nm_pgs; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Page.retrieve_cellarray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Page.goffset_obtain(file, page, j);
						String[] pl = get_payld(file, loc);
						String tb = pl[1];
						if(!tb.equals(table)){
							Page.offset_modify(file, page, i, cells[j]);
							i++;
						}
					}
					Page.modify_cellno(file, page, (byte)i);
				}
			}
			File prevfile = new File("data", table+".tbl"); 
			prevfile.delete();
		}catch(Exception e){
			System.out.println("Error at drop");
			System.out.println(e);
		}

	}

	public static String[] get_payld(RandomAccessFile file, long loc){
		String[] py_ld = new String[0];
		try{
			Long var;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);
			file.seek(loc);
			int py_dsize = file.readShort();
			int ky = file.readInt();
			int cols_num = file.readByte();
			byte[] sc = new byte[cols_num];
			int tp = file.read(sc);
			py_ld = new String[cols_num+1];
			py_ld[0] = Integer.toString(ky);
			for(int i=1; i <= cols_num; i++){
				switch(sc[i-1]){
					case 0x00:  py_ld[i] = Integer.toString(file.readByte());
								py_ld[i] = "null";
								break;

					case 0x01:  py_ld[i] = Integer.toString(file.readShort());
								py_ld[i] = "null";
								break;

					case 0x02:  py_ld[i] = Integer.toString(file.readInt());
								py_ld[i] = "null";
								break;

					case 0x03:  py_ld[i] = Long.toString(file.readLong());
								py_ld[i] = "null";
								break;

					case 0x04:  py_ld[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  py_ld[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  py_ld[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  py_ld[i] = Long.toString(file.readLong());
								break;

					case 0x08:  py_ld[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  py_ld[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  var = file.readLong();
								Date dateTime = new Date(var);
								py_ld[i] = formater.format(dateTime);
								break;

					case 0x0B:  var = file.readLong();
								Date date = new Date(var);
								py_ld[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(sc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								py_ld[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println("There is an error at getting the payload");
		}

		return py_ld;
	}


	public static void build_table(String table, String[] col){
		try{	
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int pgs_no = pages(file);
			int pg = 1;
			for(int p = 1; p <= pgs_no; p++){
				int m = Page.retrieve_right(file, p);
				if(m == 0)
			 		pg = p;
			}
			int[] arr_ky = Page.obtain_key_arr(file, pg);
			int l = arr_ky[0];
			for(int i = 0; i < arr_ky.length; i++)
				if(l < arr_ky[i])
					l = arr_ky[i];
			file.close();
			String[] tokens = {Integer.toString(l+1), table};
			place_inside("davisbase_tables", tokens);

			RandomAccessFile cfile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] cl_nm = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cp = {};
			filter(cfile, cp, cl_nm, buffer);
			l = buffer.content.size();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] token = col[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String sp = Integer.toString(i+1);
				String[] v = {Integer.toString(l), table, col_name, dt, sp, n};
				place_inside("davisbase_columns", v);
			}
			file.close();
		}catch(Exception e){
			System.out.println("There is an error at building the table");
			e.printStackTrace();
		}
	}

	public static void modify(String table, String[] set, String[] cmp){
		try{
			int k_y = new Integer(cmp[2]);
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int pgs_number = pages(file);
			int page = 1;

			for(int p = 1; p <= pgs_number; p++)
				if(Page.key_present(file, p, k_y)){
					page = p;
				}
			int[] array = Page.obtain_key_arr(file, page);
			int val = 0;
			for(int i = 0; i < array.length; i++)
				if(array[i] == k_y)
					val = i;
			int offset = Page.offset_obtain(file, page, val);
			long loc = Page.goffset_obtain(file, page, val);
			String[] array_s = retrieve_col_nmae(table);
			int num_cols = array_s.length - 1;
			String[] values = get_payld(file, loc);
			String[] type = retrieve_dat_type(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";
			for(int i = 0; i < array_s.length; i++)
				if(array_s[i].equals(set[0]))
					val = i;
			values[val] = set[2];
			String[] empty = obtain_nullable(table);

			for(int i = 0; i < empty.length; i++){
				if(values[i].equals("null") && empty[i].equals("NO")){
					System.out.println("NULL value constraint violation");
					System.out.println();
					return;
				}
			}


			byte[] sc = new byte[array_s.length-1];
			int py_sz = calci_pyld_sz(table, values, sc);
			Page.modify_lf_cel(file, page, offset, py_sz, k_y, sc, values);

			file.close();

		}catch(Exception e){
			System.out.println("There is a fault at updating the table");
			System.out.println(e);
		}
	}

	public static void place_inside(RandomAccessFile file, String table, String[] values){
		String[] typ_data = retrieve_dat_type(table);
		String[] emty = obtain_nullable(table);

		for(int i = 0; i < emty.length; i++)
			if(values[i].equals("null") && emty[i].equals("NO")){
				System.out.println("there is violation of Null Constraint");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int p_g = findkey(file, key);
		if(p_g != 0)
			if(Page.key_present(file, p_g, key)){
				System.out.println("there is violation of unique constraint");
				System.out.println();
				return;
			}
		if(p_g == 0)
			p_g = 1;


		byte[] tc = new byte[typ_data.length-1];
		short p_sz = (short) calci_pyld_sz(table, values, tc);
		int c_sz = p_sz + 6;
		int offset = Page.verify_space_branch(file, p_g, c_sz);


		if(offset != -1){
			Page.cell_add(file, p_g, offset, p_sz, key, tc, values);
		}else{
			Page.divide_branch(file, p_g);
			place_inside(file, table, values);
		}
	}
	public static void drp_to(RandomAccessFile file, String table, String[] values){
		String[] t_dat = retrieve_dat_type(table);
		String[] empt = obtain_nullable(table);

		for(int i = 0; i < empt.length; i++)
			if(values[i].equals("null") && empt[i].equals("NO")){
				System.out.println("NULL value constraint violation");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = findkey(file, key);
		if(page != 0)
			if(Page.key_present(file, page, key)){
				System.out.println("Uniqueness constraint violation");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[t_dat.length-1];
		short plSize = (short) calci_pyld_sz(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Page.verify_space_branch(file, page, cellSize);


		if(offset != -1){
			Page.cell_drop(file, page, offset, plSize, key, stc, values);
		}else{
			Page.divide_branch(file, page);
			place_inside(file, table, values);
		}
	}

	public static void place_inside(String table, String[] values){
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			place_inside(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println("There is an error at interior cell");
			e.printStackTrace();
		}
	}
	public static int calci_pyld_sz(String table, String[] vals, byte[] stc){
		String[] dat_typ = retrieve_dat_type(table);
		int sz = 1;
		sz = sz + dat_typ.length - 1;
		for(int i = 1; i < dat_typ.length; i++){
			byte tmp = stcCode(vals[i], dat_typ[i]);
			stc[i - 1] = tmp;
			sz = sz + feildLength(tmp);
		}
		return sz;
	}
	public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}
	public static byte stcCode(String val, String dataType){
		if(val.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int findkey(RandomAccessFile file, int key){
		int vl = 1;
		try{
			int pgs_nm = pages(file);
			for(int page = 1; page <= pgs_nm; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] kys = Page.obtain_key_arr(file, page);
					if(kys.length == 0)
						return 0;
					int rm = Page.retrieve_right(file, page);
					if(kys[0] <= key && key <= kys[kys.length - 1]){
						return page;
					}else if(rm == 0 && kys[kys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println("There is a fault at searching the key");
			System.out.println(e);
		}

		return vl;
	}


	public static String[] retrieve_dat_type(String table){
		String[] dat_typ = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] col_nam = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cp = {"table_name","=",table};
			filter(file, cp, col_nam, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dat_typ = array.toArray(new String[array.size()]);
			file.close();
			return dat_typ;
		}catch(Exception e){
			System.out.println("Error at getDataType");
			System.out.println(e);
		}
		return dat_typ;
	}

	public static String[] retrieve_col_nmae(String table){
		String[] c = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}catch(Exception e){
			System.out.println("there is an error at retieving the column name");
			System.out.println(e);
		}
		return c;
	}

	public static String[] obtain_nullable(String table){
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("there is an error at obtaining the nullable");
			System.out.println(e);
		}
		return n;
	}

	public static void select(String table, String[] cols, String[] cmp){
		try{
			Buffer buffer = new Buffer();
			String[] columnName = retrieve_col_nmae(table);
			String[] type = retrieve_dat_type(table);

			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			sort(file, cmp, columnName, type, buffer);
			buffer.display(cols);
			file.close();
		}catch(Exception e){
			System.out.println("There is a fault at select");
			System.out.println(e);
		}
	}
	public static void sort(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Buffer buffer){
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.retrieve_cellno(file, page);

					for(int i=0; i < numCells; i++){
						long lc = Page.goffset_obtain(file, page, i);
						file.seek(lc+2); 
						int rw_num = file.readInt();
						int num_cols = new Integer(file.readByte());

						String[] py_ld = get_payld(file, lc);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								py_ld[j] = "'"+py_ld[j]+"'";
						boolean verify = verify_cp(py_ld, rw_num, cmp, columnName);
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								py_ld[j] = py_ld[j].substring(1, py_ld[j].length()-1);

						if(verify)
							buffer.add(rw_num, py_ld);
					}
				}
			}

			buffer.cl_nme = columnName;
			buffer.fmt = new int[columnName.length];

		}catch(Exception e){
			System.out.println("There is a fault at filter");
			e.printStackTrace();
		}

	}
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, Buffer buffer){
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.retrieve_cellno(file, page);

					for(int i=0; i < numCells; i++){
						long loc = Page.goffset_obtain(file, page, i);
						file.seek(loc+2); 
						int rowid = file.readInt(); 
						int num_cols = new Integer(file.readByte());
						String[] payload = get_payld(file, loc);

						boolean check = verify_cp(payload, rowid, cmp, columnName);
						if(check)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.cl_nme = columnName;
			buffer.fmt = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}
	public static int pages(RandomAccessFile file){
		int nm_pg = 0;
		try{
			nm_pg = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println("There is an fault at interior page");
		}

		return nm_pg;
	}
	public static boolean verify_cp(String[] payload, int rowid, String[] cmp, String[] columnName){

		boolean verify = false;
		if(cmp.length == 0){
			verify = true;
		}else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			if(colPos == 1){
				switch(opt){
					case "=": if(rowid == Integer.parseInt(val)) 
								verify= true;
							  else
							  	verify = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								verify = true;
							  else
							  	verify = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								verify = true;
							  else
							  	verify = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								verify = true;
							  else
							  	verify = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								verify = true;
							  else
							  	verify = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  // TODO: check the operator
								verify = true;
							  else
							  	verify = false;	
							  break;						  							  							  							
				}
			}else{
				if(val.equals(payload[colPos-1]))
					verify = true;
				else
					verify = false;
			}
		}
		return verify;
	}

	public static void begin_datastore() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		try {
			davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x02);
			int[] offset=new int[2];
			int size1=24;
			int size2=25;
			offset[0]=pageSize-size1;
			offset[1]=offset[0]-size2;
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeInt(0);
			davisbaseTablesCatalog.writeInt(10);
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeShort(offset[0]);
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(20);
			davisbaseTablesCatalog.writeInt(1); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(28);
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(21);
			davisbaseTablesCatalog.writeInt(2); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(29);
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
		try {
			davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       
			davisbaseColumnsCatalog.writeByte(0x0D);
			davisbaseColumnsCatalog.writeByte(0x08); 
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbaseColumnsCatalog.writeShort(offset[8]); 
			davisbaseColumnsCatalog.writeInt(0); 
			davisbaseColumnsCatalog.writeInt(0); 
			for(int i=0;i<9;i++)
				davisbaseColumnsCatalog.writeShort(offset[i]);
			davisbaseColumnsCatalog.seek(offset[0]);
			davisbaseColumnsCatalog.writeShort(33); 
			davisbaseColumnsCatalog.writeInt(1); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
			davisbaseColumnsCatalog.writeBytes("rowid"); 
			davisbaseColumnsCatalog.writeBytes("INT"); 
			davisbaseColumnsCatalog.writeByte(1); 
			davisbaseColumnsCatalog.writeBytes("NO"); 
			
			davisbaseColumnsCatalog.seek(offset[1]);
			davisbaseColumnsCatalog.writeShort(39); 
			davisbaseColumnsCatalog.writeInt(2); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
			davisbaseColumnsCatalog.writeBytes("table_name"); 
			davisbaseColumnsCatalog.writeBytes("TEXT"); 
			davisbaseColumnsCatalog.writeByte(2); 
			davisbaseColumnsCatalog.writeBytes("NO"); 
			
			davisbaseColumnsCatalog.seek(offset[2]);
			davisbaseColumnsCatalog.writeShort(34); 
			davisbaseColumnsCatalog.writeInt(3); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[3]);
			davisbaseColumnsCatalog.writeShort(40); 
			davisbaseColumnsCatalog.writeInt(4); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");

			
			davisbaseColumnsCatalog.seek(offset[4]);
			davisbaseColumnsCatalog.writeShort(41); 
			davisbaseColumnsCatalog.writeInt(5); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[5]);
			davisbaseColumnsCatalog.writeShort(39);
			davisbaseColumnsCatalog.writeInt(6); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[6]);
			davisbaseColumnsCatalog.writeShort(49); 
			davisbaseColumnsCatalog.writeInt(7); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[7]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(8); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}



class Buffer{
	public int r_no; 
	public HashMap<Integer, String[]> content;
	public int[] fmt; 
	public String[] cl_nme; 
	public Buffer(){
		r_no = 0;
		content = new HashMap<Integer, String[]>();
	}
	public void add(int rowid, String[] val){
		content.put(rowid, val);
		r_no = r_no + 1;
	}
	public void modifyformat(){
		for(int i = 0; i < fmt.length; i++)
			fmt[i] = cl_nme[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(fmt[j] < i[j].length())
					fmt[j] = i[j].length();
		}
	}
	public String fix(int l, String s) {
		return String.format("%-"+(l+3)+"s", s);
	}
	public String line(String s,int len) {
		String a = "";
		for(int i=0;i<len;i++) {
			a += s;
		}
		return a;
	}

	public void display(String[] col){
		if(r_no == 0){
			System.out.println("There is an null group");
		}else{
			modifyformat();
			if(col[0].equals("*")){
				for(int l: fmt)
					System.out.print(line("-", l+3));
				System.out.println();
				for(int j = 0; j < cl_nme.length; j++)
					System.out.print(fix(fmt[j], cl_nme[j])+"|");
				System.out.println();
				for(int l: fmt)
					System.out.print(line("-", l+3));
				System.out.println();
				for(String[] i : content.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(fmt[j], i[j])+"|");
					System.out.println();
				}
				System.out.println();
			}else{
				int[] cl= new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < cl_nme.length; i++)
						if(col[j].equals(cl_nme[i]))
							cl[j] = i;
				for(int j = 0; j < cl.length; j++)
					System.out.print(line("-", fmt[cl[j]]+3));
				System.out.println();
				for(int j = 0; j < cl.length; j++)
					System.out.print(fix(fmt[cl[j]], cl_nme[cl[j]])+"|");
				System.out.println();
				for(int j = 0; j < cl.length; j++)
					System.out.print(line("-", fmt[cl[j]]+3));
				System.out.println();
				for(String[] i : content.values()){
					for(int j = 0; j < cl.length; j++)
						System.out.print(fix(fmt[cl[j]], i[cl[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}