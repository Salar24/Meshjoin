/* MESH JOIN ALGORITHM
 * Pre Processing :
 * Get Data from MySQL through JDBC connector.
 * Populate Customer, Products and Transaction ArrayLists from their respective Tables.
 * -----------------------------------------------
 * Algorithm :
 * 1. Insert Number of Partitions As Input.
 * 2. Split the Master Data into input number of partitions.
 * 3. Insert next value from Transaction's list to Queue.
 * 4. Read the next partition from MasterData.
 * 5. Populate MasterData's Hash table by next Partition of Values.
 * 6. Populate Transaction's Hash table by All values present in Queue.
 * 7. Compare CUSTOMER_ID in MasterData Hash table with Transaction's Hash table. If found, insert values into Corresponding Queue Index.
 * 8. Compare PRODUCT_ID in MasterData Hash table with Transaction's Hash Table. If found, insert values into Corresponding Queue Index.
 * 9. Compare length of Queue with Partition size. If greater, Pop the head of the Queue.
 * 10. Remove values present in Transaction's Hash Table that are not present in Queue.
 * 11. If Queue is Empty, terminate.
 *
 * 9. Go to Step Number 1
 */



import java.sql.*;

import java.util.*;  
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.time.LocalDate;
public class Main {
	
	public static String[] getDividedDate (String args) {
		//System.out.println(args);
		String Day = "";
		String Month = "";
		String Year = "";
		int slashChecker = 0;
		for(int i=0;i<args.length(); i++)
		{
			//System.out.println(args.charAt(i));
			if(args.charAt(i) == '-')
			{
				i++;
				slashChecker++;
				//System.out.println("Slash Added");
				//System.out.println(Year);
			}

			if(slashChecker == 0)	
			{
				//System.out.println("Year Added");
				Year += args.charAt(i);
				//System.out.println(Year);
			}
			if(slashChecker == 1)
			{
				//System.out.println("Month Added");
				Month += args.charAt(i);
			}
			if(slashChecker == 2)
			{
				//System.out.println("Day Added");
				Day += args.charAt(i);
			}
		}
		//System.out.println(Day);
		//System.out.println(Month);
		//System.out.println(Year);
		String[] myNum = {Year, Month, Day};
		return myNum;
	}

	public static void main(String args[]) {
		System.out.println("Working");
		try {
			System.out.println("Enter Number Of Partitions");
			Scanner sc= new Scanner(System.in);  
			int partitions = sc.nextInt();
			
			ArrayList<customers> customer_list = new ArrayList<customers>();
			ArrayList<products> products_list = new ArrayList<products>();
			ArrayList<transactions> transactions_list = new ArrayList<transactions>();
			ArrayList<TransformedData> transformed_list = new ArrayList<TransformedData>();
			
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Enter Database Connection String");
			String db = sc.next();
			System.out.println("Enter User Name");
			String name = sc.next();
			System.out.println("Enter User Password");
			String pass = sc.next();
			String Con = "jdbc:mysql://localhost:3306/db";
			
			Connection con = DriverManager.getConnection(db,name,pass);
			Statement stmt = con.createStatement();
			String sql = "select * from CUSTOMERS";
			ResultSet rs  = stmt.executeQuery(sql);
			//rs.next();
			while(rs.next())
			{
				customers C = new customers();
				C.CUSTOMER_ID = rs.getString(1);
				C.CUSTOMER_NAME = rs.getString(2);
			customer_list.add(C);
			}
			sql = "select * from products";
			rs  = stmt.executeQuery(sql);
			//rs.next();
			while(rs.next())
			{
				products P = new products();
				P.PRODUCT_ID = rs.getString(1);
				P.PRODUCT_NAME = rs.getString(2);
				P.SUPPLIER_ID = rs.getString(3);
				P.SUPPLIER_NAME = rs.getString(4);
				P.PRICE = rs.getFloat(5);
				products_list.add(P);
			}
			sql = "select * from transactions";
			rs  = stmt.executeQuery(sql);
			//rs.next();
			while(rs.next())
			{
				transactions T = new transactions();
				T.TRANSACTION_ID = rs.getInt(1);
				T.PRODUCT_ID = rs.getString(2);
				T.CUSTOMER_ID = rs.getString(3);
				T.STORE_ID = rs.getString(4);
				T.STORE_NAME = rs.getString(5);
				T.TIME_ID = rs.getString(6);
				T.T_DATE = rs.getString(7);
				T.QUANTITY = rs.getInt(8);
				transactions_list.add(T);
			}
			for(int i=0;i<transactions_list.size(); i++)
			{
				TransformedData TF = new TransformedData();
				transactions temp = new transactions();
				temp = transactions_list.get(i);
				TF.SALE_ID = String.valueOf(i);
				TF.TRANSACTION_ID = String.valueOf(temp.TRANSACTION_ID);
				TF.CUSTOMER_ID = temp.CUSTOMER_ID;
				TF.PRODUCT_ID = temp.PRODUCT_ID;
				TF.DATE_ID = temp.TIME_ID;
				TF.DATE = temp.T_DATE;
				TF.STORE_ID = temp.STORE_ID;
				TF.STORE_NAME = temp.STORE_NAME;
				TF.QUANTITY = temp.QUANTITY;
				transformed_list.add(TF);
			}
			//END OF GETTING DATA FROM DB
			int partitions_collumns = (customer_list.size() + products_list.size() + 2)/partitions;
			System.out.println("The Partition collums are " + partitions_collumns);
			System.out.println("Creating the Partition data list");
			int customer_chunk = (customer_list.size() + 1)/partitions_collumns;
			int product_chunk = (products_list.size() + 1)/partitions_collumns;
			System.out.println("Customer Chunk  = " + customer_chunk + " | Product Chunk = " + product_chunk);
			HashMap<String , Queue_Hashtable_Data> transactionHashtable = new HashMap<String , Queue_Hashtable_Data>();
			ArrayList<Queue_Hashtable_Data> TransformedData = new ArrayList<Queue_Hashtable_Data>();
			HashMap<String, String> partitionHashtable = new HashMap<String, String>();
			int customer_index = 0;
			int customer_max = customer_list.size();
			System.out.println(customer_list.size());
			int product_index = 0;
			int product_max = products_list.size();
			System.out.println(products_list.size());
			//sample loop transactions_list.size() + partitions
			Queue<Queue_Data> Stream = new LinkedList<>();
			for(int i=0;i < ( transactions_list.size() + partitions);i++)
			{
				//FOR INSERTING CUTOMERS AND PRODUCTS INTO PARTITION HASH TABLE
				partitionHashtable.clear();
				//transactionHashtable.clear();
				int k = i % partitions; 
				//System.out.println(k);
				if(k < customer_chunk)
				{
					if(customer_index == customer_max)
					{
						//System.out.println("Maximum Customer");
						customer_index = 0;
					}
					for(int l = 0; l < partitions_collumns; l++)
					{
						

						partitionHashtable.put(String.valueOf(customer_index), customer_list.get(customer_index).CUSTOMER_ID);
						
						//System.out.print(customer_list.get(customer_index).CUSTOMER_ID);
						//System.out.print(customer_index);
						customer_index++;
					}
					//System.out.println("Outside-----------Customer");
					
				}
				if(k >= customer_chunk && k < partitions)
				{
					if(product_index == product_max)
					{
						//System.out.println("Maximum Product");
						product_index = 0;
					}
					for(int l = 0; l < partitions_collumns; l++)
					{
						
						//System.out.println("Printing");
						partitionHashtable.put(String.valueOf(product_index), products_list.get(product_index).PRODUCT_ID);
						//System.out.println(products_list.get(product_index).PRODUCT_ID);
						//System.out.println(product_index);
						product_index++;
					}
					//System.out.println("Outside-----------Product");
				}
				
				
					
				//-----------------//
				//ADDING NEW ELEMENT TO QUE AND POPPING HEAD -- NORMAL WORKING//
				if(i >= partitions && i < transactions_list.size())
				{
					 Queue_Data tempQ = Stream.poll() ;
					transactionHashtable.remove(String.valueOf(tempQ.Index));
					//System.out.println("Removing");
					Queue_Data Q = new Queue_Data();
					Q.PRODUCT_ID = transactions_list.get(i).PRODUCT_ID;
					Q.CUSTOMER_ID = transactions_list.get(i).CUSTOMER_ID;
					Q.Index = i;
					
					Stream.add(Q)
					;
					Queue_Hashtable_Data QHD = new Queue_Hashtable_Data();
					QHD.Index = String.valueOf(i);
					QHD.CUSTOMER_ID = Q.CUSTOMER_ID;
					QHD.PRODUCT_ID = Q.PRODUCT_ID;
					transactionHashtable.put(String.valueOf(i), QHD);
				}
				//----------------//
				//FOR ELEMENTS < PARTITIONS. NO REMOVAL NEEDED
				if(i < partitions)
				{
				//System.out.println("Adding Transaciton to hash table : " + i );
					
				Queue_Data Q = new Queue_Data();
				Q.PRODUCT_ID = transactions_list.get(i).PRODUCT_ID;
				Q.CUSTOMER_ID = transactions_list.get(i).CUSTOMER_ID;
				Q.STORE_ID = transactions_list.get(i).STORE_ID;
				Q.SALE_ID = String.valueOf(i);
				Q.Index = i;
				Q.SUPPLIER_ID = "";
				Q.DATE_ID = transactions_list.get(i).TIME_ID;
				Q.Sales = 0;
				Q.TRANSACTION_ID = transactions_list.get(i).TRANSACTION_ID;
				Stream.add(Q);
				Queue_Hashtable_Data QHD = new Queue_Hashtable_Data();
				QHD.Index = String.valueOf(i);
				QHD.CUSTOMER_ID = Q.CUSTOMER_ID;
				QHD.PRODUCT_ID = Q.PRODUCT_ID;
				//System.out.println(Q.CUSTOMER_ID);
				transactionHashtable.put(String.valueOf(i), QHD);
				//System.out.println(transactionHashtable.size());
				
				//-----------------//
				//FOR LAST ELEMENTS = PARTITION SIZE//
				}	
				if(i >= transactions_list.size())
				{
					
						Queue_Data tempQ = Stream.remove() ;
						//System.out.println(transactionHashtable.get(String.valueOf(tempQ.Index)).CUSTOMER_ID);
						transactionHashtable.remove(String.valueOf(tempQ.Index));
						 
				}
				//System.out.println("Iteration : " + i);
				//System.out.println("Printing The Masterdata/Partition Hash table");
				//System.out.println(partitionHashtable);
				//System.out.println("Printing The Transaction Hash table");
				//System.out.println(transactionHashtable.keySet());
				//System.out.println("Printing the current state of the Queue");
				
				//JOINING THE HASH TABLES/ COMPARING THEM
				Set<String> setOfKeys = transactionHashtable.keySet();
				
		        for (String Key : setOfKeys) {
		        	Queue_Hashtable_Data QD = new Queue_Hashtable_Data();
		        	//System.out.println("Customer ID - " + transactionHashtable.get(Key).CUSTOMER_ID);
		        	//System.out.println("Product ID = " + transactionHashtable.get(Key).PRODUCT_ID);
		        	//System.out.println();
		        	QD = transactionHashtable.get(Key);
		        	
		        	if(partitionHashtable.containsValue(QD.CUSTOMER_ID) )
		        	{
		        		//-//
		        		//System.out.println("Found Customer");
		        		Set<String> Keys = partitionHashtable.keySet();
		        		for(String value : Keys)
		        		{
		        			String ID;
		        			ID = partitionHashtable.get(value);
		        			//System.out.println(QD.CUSTOMER_ID + "//");
		        			//System.out.println(ID);
		        			if(ID.equals(QD.CUSTOMER_ID))
		        			{
		        				//System.out.println("Here matching values For Customer");
		        				//System.out.println("Customer Key : " + value + " Value : " + ID);
		        				//System.out.println(Integer.valueOf(value));
		        				//System.out.println(customer_list.get(Integer.valueOf(value)).CUSTOMER_NAME);
		        				transformed_list.get(Integer.valueOf(Key)).CUSTOMER_NAME = customer_list.get(Integer.valueOf(value)).CUSTOMER_NAME;
		        			}
		        			
		        		}

		        		
		        	}
		        	if(partitionHashtable.containsValue(QD.PRODUCT_ID))
		        	{
		        		//System.out.println("Found Product");
		        		Set<String> Keys = partitionHashtable.keySet();
		        		for(String value : Keys)
		        		{
		        			String ID;
		        			//System.out.println(": " + partitionHashtable.get(value));
		        			ID = partitionHashtable.get(value);
		        			//System.out.println(QD.PRODUCT_ID + "//");
		        			//System.out.println(ID);;
		        			//System.out.println("::: " + QD.PRODUCT_ID);
		        			if(ID.equals(QD.PRODUCT_ID))
		        			{
		        				
		        				//System.out.println("Here matching values for Product");
		        				//System.out.println("Product Key : " + value + " Value : " + ID);
		        				//System.out.println("Product List");
		        				//System.out.println(products_list.get(Integer.valueOf(value)).PRODUCT_NAME);
		        				//System.out.println(products_list.get(Integer.valueOf(value)).PRICE);
		        				//System.out.println(products_list.get(Integer.valueOf(value)).SUPPLIER_NAME);
		        				transformed_list.get(Integer.valueOf(Key)).PRODUCT_NAME = products_list.get(Integer.valueOf(value)).PRODUCT_NAME;
		        				transformed_list.get(Integer.valueOf(Key)).SUPPLIER_ID = products_list.get(Integer.valueOf(value)).SUPPLIER_ID;
		        				transformed_list.get(Integer.valueOf(Key)).SUPPLIER_NAME = products_list.get(Integer.valueOf(value)).SUPPLIER_NAME;
		        				transformed_list.get(Integer.valueOf(Key)).PRICE = products_list.get(Integer.valueOf(value)).PRICE;
		        			}
		        			
		        		}
		        	}
		       // System.out.println("--------------------------------");
		       // System.out.println("");
				//--------------------------------------//
				
				//JOIN OR WHATEVER
					}
		        for(Queue_Data s : Stream) { 
					 // System.out.println(s.CUSTOMER_ID); 
					 // System.out.println(s.PRODUCT_ID);
					}
		}
			//----------------//
			//----------------//
			//----------------//
		for(Queue_Data s : Stream) { 
			  //System.out.println(s.CUSTOMER_ID);
			 // System.out.println(s.PRODUCT_ID); 
		}
			System.out.println("Final Queue Size is : " + Stream.size());
			ArrayList<Supplier> suppliers = new ArrayList<Supplier>();
			ArrayList<Store> stores = new ArrayList<Store>();
			ArrayList<Date> dates = new ArrayList<Date>();
			ArrayList<Sales> sales = new ArrayList<Sales>();
			System.out.println(stores.size() + "Size");
			for(int j=0;j<stores.size(); j++)
			{
				System.out.println("STORE ID : " + stores.get(j).STORE_ID);
				System.out.println("STORE ID : " + stores.get(j).STORE_NAME);
				System.out.println("----------------------");;
			}
			for(int i=0;i<transformed_list.size(); i++) 
			{
				transformed_list.get(i).TOTAL_SALE =  transformed_list.get(i).PRICE *  transformed_list.get(i).QUANTITY;
				transformed_list.get(i).TOTAL_SALE =  transformed_list.get(i).PRICE *  transformed_list.get(i).QUANTITY;
				String[] splitDate = getDividedDate(transformed_list.get(i).DATE);
				//System.out.println("Date : " + splitDate[1]);
				//
				
				String Quarter  =  "";
				if(Integer.valueOf(splitDate[1]) <= 3 )
				{
					Quarter = "1";
				}
				if(Integer.valueOf(splitDate[1]) > 3 )
				{
					Quarter = "2";
				}
				if(Integer.valueOf(splitDate[1]) > 6 )
				{
					Quarter = "3";
				}
				if(Integer.valueOf(splitDate[1]) > 9 )
				{
					Quarter = "4";
				}
				//System.out.println("Quater : " + Quarter);
				transformed_list.get(i).YYYY = splitDate[0];
				transformed_list.get(i).DD = splitDate[2];
				transformed_list.get(i).MM = splitDate[1];
				transformed_list.get(i).QTR = Quarter;
		
				int month = Integer.valueOf(splitDate[1]);
				int day = 0;
				LocalDate someDate = LocalDate.of(Integer.valueOf(splitDate[0]), month, Integer.valueOf(splitDate[2]));
				//System.out.println(someDate.getDayOfWeek().toString());
				if(someDate.getDayOfWeek().toString().equals("MONDAY"))
				{
					day = 1;
				}
				if(someDate.getDayOfWeek().toString().equals("TUESDAY"))
				{
					day = 2;
				}
				if(someDate.getDayOfWeek().toString().equals("WEDNESDAY"))
				{
					day = 3;
				}
				if(someDate.getDayOfWeek().toString().equals("THURSDAY"))
				{
					day = 4;
				}
				if(someDate.getDayOfWeek().toString().equals("FRIDAY"))
				{
					day = 5;
				}
				if(someDate.getDayOfWeek().toString().equals("SATURDAY"))
				{
					day = 6;
				}
				if(someDate.getDayOfWeek().toString().equals("SUNSDAY"))
				{
					day = 7;
				}
				transformed_list.get(i).WEEKDAY = String.valueOf(day);
				
//				System.out.println("Sale-ID : " + transformed_list.get(i).SALE_ID + " ID : " + transformed_list.get(i).TRANSACTION_ID + " || " +
//						   "Pr-ID : " + transformed_list.get(i).PRODUCT_ID + " || " +
//						   "Pr-Name " + transformed_list.get(i).PRODUCT_NAME + " || " +
//						   "Cu-ID : " + transformed_list.get(i).CUSTOMER_ID + " || " +
//						   "Cu-Name : " + transformed_list.get(i).CUSTOMER_NAME + " || " +
//						   "Supp-ID : " + transformed_list.get(i).SUPPLIER_ID + " || " +
//						   "Supp-Name : " + transformed_list.get(i).SUPPLIER_NAME + " || " +
//						   "Date : " + transformed_list.get(i).DATE + " || " +
//						   "Date-ID : " + transformed_list.get(i).DATE_ID + " || " +
//						   "Price : " + transformed_list.get(i).PRICE + " || " +
//						   "Qty : " + transformed_list.get(i).QUANTITY +
//						   "Sto-ID : " + transformed_list.get(i).STORE_ID+  " || " +
//						   "Sto-Name : " + transformed_list.get(i).STORE_NAME + " || " +
//						   "$$ : " + transformed_list.get(i).TOTAL_SALE + " || " +
//						   "Week Day" + transformed_list.get(i).WEEKDAY
//						   );
				//FOR STORES
			Store temp_p = new Store();
			temp_p.STORE_ID = transformed_list.get(i).STORE_ID;
			temp_p.STORE_NAME =  transformed_list.get(i).STORE_NAME;
			int check = 0;
			for(int k=0;k<stores.size();k++)
			{
				if(stores.get(k).STORE_ID.equals(temp_p.STORE_ID))
				{
					System.out.println("Matched");
					check = 1;
				}
			}
			if(check == 0)
			{
				System.out.println("New");
				stores.add(temp_p);
			}
			//FOR SALES
			Sales temp_s = new Sales();
			temp_s.CUSTOMER_ID = transformed_list.get(i).CUSTOMER_ID;
			temp_s.DATE_ID = transformed_list.get(i).DATE_ID;
			temp_s.PRODUCT_ID = transformed_list.get(i).PRODUCT_ID;
			temp_s.SUPPLIER_ID = transformed_list.get(i).SUPPLIER_ID;
			temp_s.STORE_ID = transformed_list.get(i).STORE_ID;
			temp_s.TRANSACTION_ID = transformed_list.get(i).TRANSACTION_ID;
			temp_s.SALE_ID = transformed_list.get(i).SALE_ID;
			temp_s.TOTAL_SALE = transformed_list.get(i).TOTAL_SALE;
			check = 0;
			for(int k=0;k<sales.size(); k++)
			{
				if(sales.get(k).SALE_ID.equals(temp_s.SALE_ID))
				{
					check = 1;
				}
			}
			if(check == 0)
			{
				sales.add(temp_s);
			}
			//FOR SUPPLIERS
			Supplier temp_supp =new Supplier();
			temp_supp.SUPPLIER_ID =  transformed_list.get(i).SUPPLIER_ID;
			temp_supp.SUPPLIER_NAME =  transformed_list.get(i).SUPPLIER_NAME;
			check = 0;
			for(int k=0;k<suppliers.size();k++)
			{
				if(suppliers.get(k).SUPPLIER_ID.equals(temp_supp.SUPPLIER_ID))
				{
					System.out.println("Matched");
					check = 1;
				}
			}
			if(check == 0)
			{
				System.out.println("New");
				suppliers.add(temp_supp);
			}
			//FOR DATES
			Date temp_d = new Date();
			temp_d.DATE_ID = transformed_list.get(i).DATE_ID;
			temp_d.DATE = transformed_list.get(i).DATE;
			temp_d.DD = transformed_list.get(i).DD;
			temp_d.MM = transformed_list.get(i).MM;
			temp_d.YYYY = transformed_list.get(i).YYYY;
			temp_d.WEEKDAY = transformed_list.get(i).WEEKDAY;
			temp_d.QTR = transformed_list.get(i).QTR;
			check = 0;
			for(int k=0;k<dates.size();k++)
			{
				if(dates.get(k).DATE_ID.equals(temp_d.DATE_ID))
				{
					//System.out.println("Matched");
					check = 1;
				}
			}
			if(check == 0)
			{
				//System.out.println("New");
				dates.add(temp_d);
			}
			
			//DATA INSERTION INTO MYSQL TABLES
		
			//Transactions/Cutomers/Products Done

			
			
		}
//			for(int i=0;i<stores.size();i++)
//			{
//				System.out.println("store id : " + stores.get(i).STORE_ID + " | store name : " + stores.get(i).STORE_NAME);
//			}
//			System.out.println(stores.size());
//			for(int i=0;i<sales.size();i++)
//			{
//				System.out.println("store id : " + sales.get(i).STORE_ID + " | store name : " + sales.get(i).TRANSACTION_ID);
//			}
//			System.out.println(sales.size());
//			for(int i=0;i<suppliers.size();i++)
//			{
//				System.out.println("suppplir id : " + suppliers.get(i).SUPPLIER_ID + " | supplier name : " + suppliers.get(i).SUPPLIER_NAME);
//			}
//			System.out.println(sales.size());
//			for(int i=0;i<dates.size();i++)
//			{
//				System.out.println("Date id : " + dates.get(i).DATE_ID );
//			}
//			System.out.println(dates.size());
//			for(int i=0;i<customer_list.size();i++)
//				{
//					System.out.println("customer id : " + customer_list.get(i).CUSTOMER_ID );
//				}
//				System.out.println(customer_list.size());
//			for(int i=0;i<products_list.size();i++)
//			{
//				System.out.println("product id : " + products_list.get(i).PRODUCT_ID);
//			}
//			System.out.println(products_list.size());
//			for(int i=0;i<transactions_list.size();i++)
//			{
//				System.out.println("T id : " + transactions_list.get(i).TRANSACTION_ID );
//			}
//			System.out.println(transactions_list.size());
//		
			//-----------------INSERTING INTO SQL STAR SCHEMA
			System.out.println("Enter New Schema details for Warehouse. Press 1 if same. Press 2 if not same");
			int check = sc.nextInt();
			if(check != 1)
			{
			System.out.println("Enter Database Connection String");
			String db1 = sc.next();
			System.out.println("Enter User Name");
			String name1 = sc.next();
			System.out.println("Enter User Password");
			String pass1 = sc.next();
			con = DriverManager.getConnection(db1,name1,pass1);
			}
			else {
				 con = DriverManager.getConnection(db,name,pass);
			}
			
			 try {
				 
		for(int i=0;i<stores.size();i++)
		{
							String sql_query = " INSERT IGNORE INTO STORE (STORE_ID, STORE_NAME)"
                       + " values (?, ?)";
				
				PreparedStatement preparedStmt = con.prepareStatement(sql_query);
                ((PreparedStatement) preparedStmt).setString(1, stores.get(i).STORE_ID);
                ((PreparedStatement) preparedStmt).setString(2, stores.get(i).STORE_NAME);

                //Inserting Transformed Data
                preparedStmt.execute();
				System.out.println(sql);
			
			}
			System.out.println("Hell9o");
			 }
			 catch(Exception e) {
				 System.out.print(e);
			 }
			 try {
				 
					for(int i=0;i<customer_list.size();i++)
					{
										String sql_query = " INSERT IGNORE INTO CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME)"
			                       + " values (?, ?)";
							
							PreparedStatement preparedStmt = con.prepareStatement(sql_query);
			                ((PreparedStatement) preparedStmt).setString(1, customer_list.get(i).CUSTOMER_ID);
			                ((PreparedStatement) preparedStmt).setString(2, customer_list.get(i).CUSTOMER_NAME);

			                //Inserting Transformed Data
			                preparedStmt.execute();
							System.out.println(sql);
						
						}
						System.out.println("Hell9o");
						 }
						 catch(Exception e) {
							 System.out.print(e);
						 }
			 try {
				 
					for(int i=0;i<dates.size();i++)
					{
						
						String sql_query = " INSERT IGNORE INTO DATE (DATE_ID, DD, MM, YYYY, QTR, WEEKDAY)"
		                        + " values (?, ?, ?, ?, ?, ?)";
						
						PreparedStatement preparedStmt = con.prepareStatement(sql_query);
		                ((PreparedStatement) preparedStmt).setString(1, dates.get(i).DATE_ID);
		                ((PreparedStatement) preparedStmt).setString(2, dates.get(i).DD);
		                ((PreparedStatement) preparedStmt).setString(3, dates.get(i).MM);
		                ((PreparedStatement) preparedStmt).setString(4, dates.get(i).YYYY);
		                ((PreparedStatement) preparedStmt).setString(5, dates.get(i).QTR);
		                ((PreparedStatement) preparedStmt).setString(6, dates.get(i).WEEKDAY);
		
		                //Inserting Transformed Data
		                preparedStmt.execute();
						System.out.println(sql);
					
					}
					System.out.println("Hell9o");
					 }
					 catch(Exception e) {
						 System.out.print(e);
					 }
			 try {
				 
					for(int i=0;i<products_list.size();i++)
					{
						
						String sql_query = " INSERT IGNORE INTO PRODUCT (PRODUCT_ID, PRODUCT_NAME)"
		                        + " values (?, ?)";
						
						PreparedStatement preparedStmt = con.prepareStatement(sql_query);
		                ((PreparedStatement) preparedStmt).setString(1, products_list.get(i).PRODUCT_ID);
		                ((PreparedStatement) preparedStmt).setString(2, products_list.get(i).PRODUCT_NAME);
		
		                //Inserting Transformed Data
		                preparedStmt.execute();
						System.out.println(sql);
				
				}
					System.out.println("Hell9o");
					 }
					 catch(Exception e) {
						 System.out.print(e);
					 }
			 try {
				 
					for(int i=0;i<suppliers.size();i++)
					{
						
						String sql_query = " INSERT IGNORE INTO SUPPLIER (SUPPLIER_ID, SUPPLIER_NAME)"
		                        + " values (?, ?)";
						
						PreparedStatement preparedStmt = con.prepareStatement(sql_query);
		                ((PreparedStatement) preparedStmt).setString(1, suppliers.get(i).SUPPLIER_ID);
		                ((PreparedStatement) preparedStmt).setString(2, suppliers.get(i).SUPPLIER_NAME);
		
	                //Inserting Transformed Data
		                preparedStmt.execute();
						System.out.println(sql);
					
					}
					System.out.println("Hell9o");
					 }
					 catch(Exception e) {
						 System.out.print(e);
					 }
			 try {
				 
					for(int i=0;i<transactions_list.size();i++)
					{
						
						String sql_query = " INSERT IGNORE INTO TRANSACTION (TRANSACTION_ID, PRODUCT_ID, CUSTOMER_ID, STORE_ID, STORE_NAME, TIME_ID, T_DATE, QUANTITY )"
		                        + " values (?, ?, ?, ?, ?, ?, ?, ?)";
						
						PreparedStatement preparedStmt = con.prepareStatement(sql_query);
		                ((PreparedStatement) preparedStmt).setInt(1, transactions_list.get(i).TRANSACTION_ID);
	                ((PreparedStatement) preparedStmt).setString(2, transactions_list.get(i).PRODUCT_ID);
		                ((PreparedStatement) preparedStmt).setString(3, transactions_list.get(i).PRODUCT_ID);
		                ((PreparedStatement) preparedStmt).setString(4, transactions_list.get(i).STORE_ID);
		                ((PreparedStatement) preparedStmt).setString(5, transactions_list.get(i).STORE_NAME);
		                ((PreparedStatement) preparedStmt).setString(6, transactions_list.get(i).TIME_ID);
		                ((PreparedStatement) preparedStmt).setString(7, transactions_list.get(i).T_DATE);
		                ((PreparedStatement) preparedStmt).setInt(8, transactions_list.get(i).QUANTITY);
		
		                //Inserting Transformed Data
		                preparedStmt.execute();
						System.out.println(sql);
					
				}
					System.out.println("Hell9o");
					 }
					 catch(Exception e) {
						 System.out.print(e);
					 }
			 //con = DriverManager.getConnection("jdbc:mysql://localhost:3306/star_schema","root","123456");
			 try
		 {
				 
			 for(int i=0;i<sales.size();i++)
				{
					
					String sql_query = " INSERT IGNORE INTO SALES (SALE_ID, CUSTOMER_ID, PRODUCT_ID, DATE_ID, SUPPLIER_ID, STORE_ID, TRANSACTION_ID, TOTAL_SALE)"
	                        + " values (?, ? , ? ,? ,? ,? ,? ,?)";
					
					PreparedStatement preparedStmt = con.prepareStatement(sql_query);
	                ((PreparedStatement) preparedStmt).setString(1, sales.get(i).SALE_ID);
	                ((PreparedStatement) preparedStmt).setString(2, sales.get(i).CUSTOMER_ID);
	                ((PreparedStatement) preparedStmt).setString(3, sales.get(i).PRODUCT_ID);
	                ((PreparedStatement) preparedStmt).setString(4, sales.get(i).DATE_ID);
	                ((PreparedStatement) preparedStmt).setString(5, sales.get(i).SUPPLIER_ID);
	                ((PreparedStatement) preparedStmt).setString(6, sales.get(i).STORE_ID);
	                ((PreparedStatement) preparedStmt).setInt(7, Integer.valueOf(sales.get(i).TRANSACTION_ID));
	                ((PreparedStatement) preparedStmt).setFloat(8, sales.get(i).TOTAL_SALE);

	                //Inserting Transformed Data
	                preparedStmt.execute();
					System.out.println(sql);
				
				}
				 }
				 catch(Exception e) {
					 System.out.print(e);
				 }
			 
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		
	}
	
}





