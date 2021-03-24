import java.sql.*;

public class MariaDBConnect {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		final String driver = "org.mariadb.jdbc.Driver";
		final String DB_IP = "localhost";
		final String DB_PORT = "3306";
		final String DB_NAME = "corona";
		final String DB_URL = 
				"jdbc:mariadb://" + DB_IP + ":" + DB_PORT + "/" + DB_NAME;

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(DB_URL, "root", "password");
			if (conn != null) {
				System.out.println("DB connected!");
			}

		} catch (ClassNotFoundException e) {
			System.out.println("Driver loading failed");
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("DB connection failed");
			e.printStackTrace();
		}

		try {
			String sql = "select * from estimator";

			pstmt = conn.prepareStatement(sql);

			rs = pstmt.executeQuery();
			String userId = null;
			String password = null;
			String name = null;
			while (rs.next()) {
				userId = rs.getString(1);
				password = rs.getString(2);
				name = rs.getString(3);
			}

			System.out.println(userId);
			System.out.println(password);
			System.out.println(name);

		} catch (SQLException e) {
			System.out.println("error: " + e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}

				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
