/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1

	try{
		
		String make, model, age, seats, select_max;
		int ID = 0;

		System.out.print("\tPlease enter the airplane make: ");
		make = in.readLine();
		System.out.print("\tPlease enter the airplane model: ");
		model = in.readLine();
		System.out.print("\tPlease enter the airplane age: ");
		age = in.readLine();
		System.out.print("\tPlease enter the number of seats: ");
		seats = in.readLine();
		
		String query = "INSERT INTO Plane(make, model, age, seats) VALUES ( \'" + make + "\', \'" + model + "\', " + age + "," + seats + ");";
		esql.executeUpdate(query);

		select_max = "SELECT MAX(P.id) FROM Plane P;";
		List<List<String>> max = esql.executeQueryAndReturnResult(select_max);

		for(List<String> sl : max){
			for(String s: sl){
				ID = Integer.parseInt(s);
			}
		}

		String test = "SELECT * FROM Plane;";
		esql.executeQueryAndPrintResult(test);
		
		System.out.println("\nNew airplane is added as below:\n");
		String result = "SELECT * FROM Plane WHERE id = " + ID + ";";
		esql.executeQueryAndPrintResult(result);
		System.out.println("\n");
	}//end try
	catch(Exception e){
		System.out.println(e.getMessage());
	}

	}

	public static void AddPilot(DBproject esql) {//2

	try{
		
		String fullname, nationality, select_max;
		int ID = 0;

		System.out.print("\tPlease enter pilot fullname (Firstname Lastname): ");
		fullname = in.readLine();
		System.out.print("\tPlease enter nationality: ");
		nationality = in.readLine();

		String query = "INSERT INTO Pilot(fullname, nationality) VALUES ( \'" + fullname + "\', \'" + nationality + "\');";
		esql.executeUpdate(query);

		select_max = "SELECT MAX(P.id) FROM Pilot P;";
		List<List<String>> max = esql.executeQueryAndReturnResult(select_max);

		for(List<String> sl : max){
			for(String s: sl){
				ID = Integer.parseInt(s);
			}
		}

		String test = "SELECT * FROM Pilot;";
		esql.executeQueryAndPrintResult(test);
		
		System.out.println("\nNew pilot is added as below:\n");
		String result = "SELECT * FROM Pilot WHERE id = " + ID + ";";
		esql.executeQueryAndPrintResult(result);
		System.out.println("\n");
	}//end try
	catch(Exception e){
		System.out.println(e.getMessage());
	}
	}
	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB

	try{
		
		String actual_departure_date, actual_arrival_date, arrival_airport, departure_airport, select_max;
		int cost, num_sold, num_stops;
		int fnum = 0;

		System.out.print("\tPlease enter the cost: ");
		cost = Integer.parseInt(in.readLine());
		System.out.print("\tPlease enter the number of unavailable seats : ");
		num_sold = Integer.parseInt(in.readLine());
		System.out.print("\tPlease enter the number of stops: ");
		num_stops = Integer.parseInt(in.readLine());
		System.out.print("\tPlease enter the departure date: ");
		actual_departure_date = in.readLine();
		System.out.print("\tPlease enter the arrival date: ");
		actual_arrival_date = in.readLine();
		System.out.print("\tPlease enter the arrival airport: ");
		arrival_airport = in.readLine();
		System.out.print("\tPlease enter the departure airport: ");
		departure_airport = in.readLine();
		
		String query = "INSERT INTO Flight(cost, num_sold, num_stops, actual_departure_date, actual_arrival_date" 
				+ ", arrival_airport, departure_airport) VALUES ( " + cost + "," + num_sold + "," + num_stops 
				+ ", \'" + actual_departure_date + "\', \'" + actual_arrival_date + "\', \' " + arrival_airport 
				+ "\', \'" + departure_airport + "\');";
		esql.executeUpdate(query);

		select_max = "SELECT MAX(fnum) FROM Flight;";
		List<List<String>> max = esql.executeQueryAndReturnResult(select_max);

		for(List<String> sl : max){
			for(String s: sl){
				fnum = Integer.parseInt(s);
			}
		}

		String test = "SELECT * FROM Flight;";
		esql.executeQueryAndPrintResult(test);
		
		System.out.println("\nNew flight is added as below:\n");
		String result = "SELECT * FROM Flight WHERE fnum = " + fnum + ";";
		esql.executeQueryAndPrintResult(result);
		System.out.println("\n");
	}//end try
	catch(Exception e){
		System.out.println(e.getMessage());
	}

	}

	public static void AddTechnician(DBproject esql) {//4

	try{
		
		String full_name, select_max;
		int ID = 0;

		System.out.print("\tPlease enter Technician fullname (Firstname Lastname): ");
		full_name = in.readLine();

		String query = "INSERT INTO Technician(full_name) VALUES ( \'" + full_name + "\');";
		esql.executeUpdate(query);

		select_max = "SELECT MAX(id) FROM Technician;";
		List<List<String>> max = esql.executeQueryAndReturnResult(select_max);

		for(List<String> sl : max){
			for(String s: sl){
				ID = Integer.parseInt(s);
			}
		}

		String test = "SELECT * FROM Technician;";
		esql.executeQueryAndPrintResult(test);
		
		System.out.println("\nNew technician is added as below:\n");
		String result = "SELECT * FROM Technician WHERE id = " + ID + ";";
		esql.executeQueryAndPrintResult(result);
		System.out.println("\n");
	}//end try
	catch(Exception e){
		System.out.println(e.getMessage());
	}
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
		//int Cid;
		
		//System.out.println("please enter your SSN: ");
		//Cid = Ineger.parseInt(in.readLine());
		
		
		 
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
	}
}
