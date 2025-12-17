    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.SQLException;

    public class ConnectionFactory {
        public static final String POSTGRESQL = "postgresql";

        public static Connection getDefaultParameterConnection(String dbType) {
            if (POSTGRESQL.equals(dbType)) {
                String url = "jdbc:postgresql://localhost:5432/postgres"; // Replace with your database URL
                String user = "postgres"; // Replace with your database username
                String password = "secret"; // Replace with your database password
                try {
                    return DriverManager.getConnection(url, user, password);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Failed to connect to the database");
                }
            } else {
                throw new UnsupportedOperationException("Database type not supported");
            }
        }
    }