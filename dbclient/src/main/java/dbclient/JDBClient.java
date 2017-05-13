package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class JDBClient {

    private static final Logger log = Logger.getLogger("JDBClient");
    private final String DATABASE_DRIVER = "org.hsqldb.jdbcDriver";
    private final String CONNECTION_URL = "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db";
    private String username;
    private String password;
    private Connection connection;

    public JDBClient(final String username, final String password) {
        this.username = username;
        this.password = password;
    }

    public boolean connectToDatabase() {
        try {
            Class.forName(DATABASE_DRIVER).newInstance();
            connection = DriverManager.getConnection(CONNECTION_URL, username, password);
            connection.setAutoCommit(true);
            log.info("Connection to database " + CONNECTION_URL + " established.");
        } catch (ClassNotFoundException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            return false;
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            return false;
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            return false;
        }

        return true;
    }

    public boolean closeConnectionToDatabase() {
        try {
            connection.close();
            log.info("Connection to database " + CONNECTION_URL + " closed.");
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            return false;
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            return false;
        }

        return true;
    }

    private void createTables() {
        try {
            String sqlCreateTableStudent = "CREATE TABLE student ( "
                    + "pkey INTEGER NOT NULL PRIMARY KEY, "
                    + "name VARCHAR(50), "
                    + "sex VARCHAR(7), "
                    + "age INTEGER NOT NULL, "
                    + "level INTEGER "
                    + ");";

            String sqlCreateTableFaculty = "CREATE TABLE faculty ("
                    + "pkey INTEGER NOT NULL PRIMARY KEY, "
                    + "name VARCHAR(50) "
                    + ");";

            String sqlCreateTableClass = "CREATE TABLE class ("
                    + "pkey INTEGER NOT NULL PRIMARY KEY, "
                    + "name VARCHAR(50), "
                    + "fkey_faculty INTEGER, "
                    + "FOREIGN KEY(fkey_faculty) REFERENCES faculty(pkey)"
                    + ");";

            String sqlCreateTableEnrollment = "CREATE TABLE enrollment ("
                    + "fkey_student INTEGER, "
                    + "fkey_class INTEGER, "
                    + "FOREIGN KEY(fkey_student) REFERENCES student(pkey), "
                    + "FOREIGN KEY(fkey_class) REFERENCES class(pkey)"
                    + ");";

            createTable(sqlCreateTableStudent);
            createTable(sqlCreateTableFaculty);
            createTable(sqlCreateTableClass);
            createTable(sqlCreateTableEnrollment);
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void createTable(final String sqlCreateTable) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sqlCreateTable);
        statement = connection.prepareStatement(sqlCreateTable);
        statement.executeUpdate();
        statement.close();
        log.info(sqlCreateTable.substring(0, sqlCreateTable.indexOf('(')));
    }

    private void dropTables() {
        try {
            dropTable("enrollment");
            dropTable("class");
            dropTable("faculty");
            dropTable("student");
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void dropTable(final String tableName) throws SQLException {
        String sqlDropTable = "DROP TABLE " + tableName;
        PreparedStatement statement = connection.prepareStatement(sqlDropTable);
        statement.executeUpdate();
        statement.close();
        log.info(sqlDropTable);
    }

    private void insertIntoTablesDataset() {
        try {
            insertIntoTableStudent(1, "John Smith", "male", 23, 2);
            insertIntoTableStudent(2, "Rebecca Milson", "female", 27, 3);
            insertIntoTableStudent(3, "George Heartbreaker", "male", 19, 1);
            insertIntoTableStudent(4, "Deepika Chopra", "female", 25, 3);
            insertIntoTableFaculty(100, "Engineering");
            insertIntoTableFaculty(101, "Philosophy");
            insertIntoTableFaculty(102, "Law and administration");
            insertIntoTableFaculty(103, "Languages");
            insertIntoTableClass(1000, "Introduction to labour law", 102);
            insertIntoTableClass(1001, "Graph algorithms", 100);
            insertIntoTableClass(1002, "Existentialism in 20th century", 101);
            insertIntoTableClass(1003, "English grammar", 103);
            insertIntoTableClass(1004, "From Plato to Kant", 101);
            insertIntoTableEnrollment(1, 1_000);
            insertIntoTableEnrollment(1, 1_002);
            insertIntoTableEnrollment(1, 1_003);
            insertIntoTableEnrollment(1, 1_004);
            insertIntoTableEnrollment(2, 1_002);
            insertIntoTableEnrollment(2, 1_003);
            insertIntoTableEnrollment(4, 1_000);
            insertIntoTableEnrollment(4, 1_002);
            insertIntoTableEnrollment(4, 1_003);
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void insertIntoTableStudent(int pkey, String name, String sex, int age, int level) throws SQLException {
        String sqlInsertIntoValues = "INSERT INTO student "
                + "VALUES(?,?,?,?,?);";
        PreparedStatement statement = connection.prepareStatement(sqlInsertIntoValues);
        statement.setInt(1, pkey);
        statement.setString(2, name);
        statement.setString(3, sex);
        statement.setInt(4, age);
        statement.setInt(5, level);
        statement.executeUpdate();
        statement.close();
        String values = pkey + ", " + name + ", " + sex + ", " + age + ", " + level;
        log.info(sqlInsertIntoValues.substring(0, sqlInsertIntoValues.indexOf('(')) + " ( " + values + " );");
    }

    private void insertIntoTableFaculty(int pkey, String name) throws SQLException {
        String sqlInsertIntoValues = "INSERT INTO faculty "
                + "VALUES(?,?);";
        PreparedStatement statement = connection.prepareStatement(sqlInsertIntoValues);
        statement.setInt(1, pkey);
        statement.setString(2, name);
        statement.executeUpdate();
        statement.close();
        String values = pkey + ", " + name;
        log.info(sqlInsertIntoValues.substring(0, sqlInsertIntoValues.indexOf('(')) + " ( " + values + " );");
    }

    private void insertIntoTableClass(int pkey, String name, int fkey_faculty) throws SQLException {
        String sqlInsertIntoValues = "INSERT INTO class "
                + "VALUES(?,?,?);";
        PreparedStatement statement = connection.prepareStatement(sqlInsertIntoValues);
        statement.setInt(1, pkey);
        statement.setString(2, name);
        statement.setInt(3, fkey_faculty);
        statement.executeUpdate();
        statement.close();
        String values = pkey + ", " + name + ", " + fkey_faculty;
        log.info(sqlInsertIntoValues.substring(0, sqlInsertIntoValues.indexOf('(')) + " ( " + values + " );");
    }

    private void insertIntoTableEnrollment(int fkey_student, int fkey_class) throws SQLException {
        String sqlInsertIntoValues = "INSERT INTO enrollment "
                + "VALUES(?,?);";
        PreparedStatement statement = connection.prepareStatement(sqlInsertIntoValues);
        statement.setInt(1, fkey_student);
        statement.setInt(2, fkey_class);
        statement.executeUpdate();
        statement.close();
        String values = fkey_student + ", " + fkey_class;
        log.info(sqlInsertIntoValues.substring(0, sqlInsertIntoValues.indexOf('(')) + " ( " + values + " );");
    }

    private void findKeysAndNamesOfPersonsRegisteredAsStudents() {
        try {
            String sqlSelect = "SELECT pkey, name "
                    + "FROM student";
            PreparedStatement statement = connection.prepareStatement(sqlSelect);
            statement.execute();
            ResultSet response = statement.getResultSet();
            log.info(sqlSelect);

            while (response.next()) {
                int pkey = response.getInt("pkey");
                String name = response.getString("name");
                System.out.println("pkey=" + pkey + ", name=" + name);
            }

            statement.close();
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void findKeysAndNamesOfPersonsNotEnrolledToAnyClasses() {
        try {
            String sqlSelect = "SELECT * FROM student s "
                    + "WHERE s.pkey NOT IN ( "
                    + "SELECT DISTINCT fkey_student FROM enrollment "
                    + ")";
            PreparedStatement statement = connection.prepareStatement(sqlSelect);
            statement.execute();
            ResultSet response = statement.getResultSet();
            log.info(sqlSelect);

            while (response.next()) {
                int pkey = response.getInt("pkey");
                String name = response.getString("name");
                System.out.println("pkey=" + pkey + ", name=" + name);
            }

            statement.close();
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void findKeysAndNamesOfFemalesLearningAboutExistentialismIn20thCentury() {
        try {
            String sqlSelect = "SELECT DISTINCT pkey, name "
                    + "FROM student s "
                    + "JOIN enrollment e ON e.fkey_student = s.pkey "
                    + "WHERE s.sex = 'female' AND e.fkey_class = ( "
                    + "SELECT pkey "
                    + "FROM class "
                    + "WHERE name = 'Existentialism in 20th century' "
                    + ") ";
            PreparedStatement statement = connection.prepareStatement(sqlSelect);
            statement.execute();
            ResultSet response = statement.getResultSet();
            log.info(sqlSelect);

            while (response.next()) {
                int pkey = response.getInt("pkey");
                String name = response.getString("name");
                System.out.println("pkey=" + pkey + ", name=" + name);
            }

            statement.close();
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void findAllFacultyNamesWhoseClassesNoOneRegisteredTo() {
        try {
            String sqlSelect = "SELECT f.name "
                    + "FROM faculty f "
                    + "JOIN class c ON c.fkey_faculty = f.pkey "
                    + "WHERE c.pkey NOT IN ( "
                    + "SELECT fkey_class "
                    + "FROM enrollment "
                    + ")";
            PreparedStatement statement = connection.prepareStatement(sqlSelect);
            statement.execute();
            ResultSet response = statement.getResultSet();
            log.info(sqlSelect);

            while (response.next()) {
                String name = response.getString("name");
                System.out.println("name=" + name);
            }

            statement.close();
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    private void findAgeOfTheEldestPersonThatIsLearningAboutLabourLaw() {
        try {
            String sqlSelect = "SELECT MAX( s.age ) AS max_age "
                    + "FROM student s "
                    + "WHERE s.pkey IN ( "
                    + "SELECT e.fkey_student "
                    + "FROM enrollment e "
                    + "WHERE e.fkey_class IN ( "
                    + "SELECT c.pkey "
                    + "FROM class c "
                    + "WHERE c.name = 'Introduction to labour law' "
                    + ")"
                    + ")";
            PreparedStatement statement = connection.prepareStatement(sqlSelect);
            statement.execute();
            ResultSet response = statement.getResultSet();
            log.info(sqlSelect);

            while (response.next()) {
                int age = response.getInt("max_age");
                System.out.println("max_age=" + age);
            }

            statement.close();
        } catch (SQLException x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        } catch (Exception x) {
            log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
        }
    }

    public static void main(String[] args) {
        JDBClient dbclient = new JDBClient("SA", "");
        final long SLEEP_TIME = 250;

        if (dbclient.connectToDatabase()) {
            try {
                dbclient.createTables();
                Thread.sleep(SLEEP_TIME);
                dbclient.insertIntoTablesDataset();
                Thread.sleep(SLEEP_TIME);
                dbclient.findKeysAndNamesOfPersonsRegisteredAsStudents();
                Thread.sleep(SLEEP_TIME);
                dbclient.findKeysAndNamesOfPersonsNotEnrolledToAnyClasses();
                Thread.sleep(SLEEP_TIME);
                dbclient.findKeysAndNamesOfFemalesLearningAboutExistentialismIn20thCentury();
                Thread.sleep(SLEEP_TIME);
                dbclient.findAllFacultyNamesWhoseClassesNoOneRegisteredTo();
                Thread.sleep(SLEEP_TIME);
                dbclient.findAgeOfTheEldestPersonThatIsLearningAboutLabourLaw();
                Thread.sleep(SLEEP_TIME);
                dbclient.dropTables();
                Thread.sleep(SLEEP_TIME);
                dbclient.closeConnectionToDatabase();
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException x) {
                log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            } catch (Exception x) {
                log.warning("[" + x.getClass().getName() + "] " + x.getMessage());
            }
        }

    }

}
