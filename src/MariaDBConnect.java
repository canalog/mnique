import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MariaDBConnect {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		final String driver = "org.mariadb.jdbc.Driver";
		final String DB_IP = "localhost";
		final String DB_PORT = "3306";
		final String DB_NAME = "nhis";
		final String DB_URL = 
				"jdbc:mariadb://" + DB_IP + ":" + DB_PORT + "/" + DB_NAME;
		
		try {
			Class.forName("org.mariadb.jdbc.Driver");
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		
		Connection conn = null;	
		Statement stmt = null;
		
		try {
			//connect DB
			conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306","root","password");
			System.out.println("Connection Success!");
			
			//create sql statements
			stmt = conn.createStatement();
			stmt.executeUpdate("use nhis");
			
			//resultsets = get the result of sql queries
			ResultSet rs = stmt.executeQuery("select * from nhis_10000");
			while(rs.next()) {
				System.out.println(rs.getString(1)+"\t"+rs.getInt(2)+"\t"+rs.getString(3)+"\t"
			+rs.getString(4)+"\t"+rs.getInt(5));
			}
		}
		catch(SQLException e) {
			e.printStackTrace();
		}

	}
}
