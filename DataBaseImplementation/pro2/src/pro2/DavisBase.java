package pro2;


import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;

public class DavisBase {
	static String prompt = "DAVISQL> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {
    	begin();
		welcomeScreen();
		String user_input = ""; 

		while(!user_input.equals("exit")) {
			System.out.print(prompt);
			user_input = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			userinput_breakdown(user_input);
		}
		System.out.println("The System is terminating");


    }
	public static void welcomeScreen() {
		System.out.println(line("-",80));
        System.out.println("DAVISBASE!!");
		System.out.println("Utilise help to show supported commands ");
		System.out.println(line("-",80));
	}
	public static String line(String str,int num) {
		String b = "";
		for(int i=0;i<num;i++) {
			b += str;
		}
		return b;
	}
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("POSSIBLE COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		
		System.out.println("\tCREATE TABLE table_name (column type_data , column type_data,...);      This command is used to create a table in the database");
		System.out.println("\tSELECT * FROM tablename;   This command is used to select a particular record from the table");
		System.out.println("\tINSERT into tablename ( column1 , column2.. ) values ( value1 , value2.... );    this command is used to specify particular records in the table");
		System.out.println("\tSHOW TABLES;                      Display all tables in the database.");
		System.out.println("\tUPDATE table_name SET col_name=value WHERE col_name1=value1;        This command is used to update specified records in the table");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  To display a record according to specified rowid");
		System.out.println("\tDROP TABLE table_name;                           Table is removed");
		//System.out.println("\tVERSION;                                         Show the program version.");
		System.out.println("\tHELP;                                            Help info is dispalyed");
		System.out.println("\tEXIT;                                            program is terminated");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	/** Display the DavisBase version */
	public static void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}
	public static boolean present_table(String table){
		boolean e = false;
		table = table+".tbl";
		try {
			File dir_info = new File("data");
			String[] prevtablefiles;
			prevtablefiles = dir_info.list();
			for (int i=0; i<prevtablefiles.length; i++) {
				if(prevtablefiles[i].equals(table))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		return e;
	}

	public static void begin(){
		try {
			File dir_info = new File("data");
			if(dir_info.mkdir()){
				System.out.println("There is no system dir ");
				System.out.println();
				Table.begin_datastore();
			}else {
				String m1 = "davisbase_columns.tbl";
				String meta2 = "davisbase_tables.tbl";
				String[] prevtableFiles = dir_info.list();
				boolean verify = false;
				for (int i=0; i<prevtableFiles.length; i++) {
					if(prevtableFiles[i].equals(m1))
						verify = true;
				}
				if(!verify){
					System.out.println("There is no system table database_colums.tbl");
					System.out.println();
					Table.begin_datastore();
				}
				verify = false;
				for (int i=0; i<prevtableFiles.length; i++) {
					if(prevtableFiles[i].equals(meta2))
						verify = true;
				}
				if(!verify){
					System.out.println("There is no system table database_tables.tbl");
					System.out.println();
					Table.begin_datastore();
				}
			}
		}catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

	}



	public static String[] equ_breakdown(String equ){
		String est[] = new String[3];
		String var[] = new String[2];
	if(equ.contains("=")) {
			var = equ.split("=");
			est[0] = var[0].trim();
			est[1] = "=";
			est[2] = var[1].trim();
		}

		if(equ.contains(">")) {
			var = equ.split(">");
			est[0] = var[0].trim();
			est[1] = ">";
			est[2] = var[1].trim();
		}

		if(equ.contains("<")) {
			var = equ.split("<");
			est[0] = var[0].trim();
			est[1] = "<";
			est[2] = var[1].trim();
		}

		if(equ.contains(">=")) {
			var = equ.split(">=");
			est[0] = var[0].trim();
			est[1] = ">=";
			est[2] = var[1].trim();
		}

		if(equ.contains("<=")) {
			var = equ.split("<=");
			est[0] = var[0].trim();
			est[1] = "<=";
			est[2] = var[1].trim();
		}

		if(equ.contains("<>")) {
			var = equ.split("<>");
			est[0] = var[0].trim();
			est[1] = "<>";
			est[2] = var[1].trim();
		}

		return est;
	}
	public static void userinput_breakdown (String user_input) {
		String[] commandTokens = user_input.split(" ");

		switch (commandTokens[0]) {
			case "start":
				Table.begin_datastore();
				break;

			case "create":
				String build_table = commandTokens[2];
				String[] temp_build = user_input.split(build_table);
				String col_var = temp_build[1].trim();
				String[] columnsnew = col_var.substring(1, col_var.length()-1).split(",");
				for(int i = 0; i < columnsnew.length; i++)
					columnsnew[i] = columnsnew[i].trim();
				if(present_table(build_table)){
					System.out.println("Table "+build_table+" already exists.");
					System.out.println();
					break;
				}
				Table.build_table(build_table, columnsnew);		
				break;

			case "drop":
				String dt = commandTokens[2];
				if(!present_table(dt)){
					System.out.println("Table "+dt+" does not exist.");
					System.out.println();
					break;
				}
				Table.drop(dt);
				break;

			case "show":
				Table.show();
				break;

			case "insert":
				String table_place = commandTokens[2];
				String place_vals = user_input.split("values")[1].trim();
				place_vals = place_vals.substring(1, place_vals.length()-1);
				String[] place_numbers = place_vals.split(",");
				for(int i = 0; i < place_numbers.length; i++)
					place_numbers[i] = place_numbers[i].trim();
				if(!present_table(table_place)){
					System.out.println("Table "+table_place+" does not exist.");
					System.out.println();
					break;
				}
				Table.place_inside(table_place, place_numbers);
				break;
			/*case "delete":
				String delete_table = commandTokens[2];
				String delete_vals = userCommand.split("WHERE")[1].trim();
				String[] delete_col = delete_vals.split("=");
				
				
				if(!present_table(delete_table)){
					System.out.println("Table "+delete_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.insertInto(delete_table, delete_values);
				break;*/

			case "update":
				String tablenew = commandTokens[1];
				String[] temporaryn1 = user_input.split("set");
				String[] temporaryn2 = temporaryn1[1].split("where");
				String modifyest = temporaryn2[1];
				String update_set_s = temporaryn2[0];
				String[] set = equ_breakdown(update_set_s);
				String[] est_update = equ_breakdown(modifyest);
				if(!present_table(tablenew)){
					System.out.println("Table "+tablenew+" does not exist.");
					System.out.println();
					break;
				}
				Table.modify(tablenew, set, est_update);
				break;
				
			case "select":
				String[] choose_val;
				String[] choose_col;
				String[] choose_var = user_input.split("where");
				if(choose_var.length > 1){
					String filter = choose_var[1].trim();
					choose_val = equ_breakdown(filter);
				}else{
					choose_val = new String[0];
				}
				String[] choose = choose_var[0].split("from");
				String choose_table = choose[1].trim();
				String choose_culmns = choose[0].replace("select", "").trim();
				if(choose_culmns.contains("*")){
					choose_col = new String[1];
					choose_col[0] = "*";
				}
				else{
					choose_col = choose_culmns.split(",");
					for(int i = 0; i < choose_col.length; i++)
						choose_col[i] = choose_col[i].trim();
				}
				if(!present_table(choose_table)){
					System.out.println("Table "+choose_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.select(choose_table, choose_col, choose_val);
				break;

			case "help":
				help();
				break;

			case "version":
				version();
				break;

			case "exit":
				break;

			default:
				System.out.println("Unfortunately the command is not present \"" + user_input + "\"");
				System.out.println();
				break;
		}
	} 
	
}