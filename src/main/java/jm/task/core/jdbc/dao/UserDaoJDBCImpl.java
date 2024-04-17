package jm.task.core.jdbc.dao;

import antlr.TokenStreamRewriteEngine;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String createUsersTableSQL = "CREATE TABLE IF NOT EXISTS users (`id` int NOT NULL AUTO_INCREMENT," +
                "`name` varchar(45) NOT NULL," +
                "`lastname` varchar(45) NOT NULL," +
                "`age` tinyint NOT NULL, PRIMARY KEY (`id`))";
        try ( Connection connection = Util.getConnection();
              Statement statement = connection.createStatement()) {
            statement.executeUpdate(createUsersTableSQL);
            connection.commit();
        } catch (SQLException e) {
            System.err.println(e);
        }
    }

    public void dropUsersTable() {
        String dropUsersTableSQL = "DROP TABLE IF EXISTS users";
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(dropUsersTableSQL);
            connection.commit();
        } catch (SQLException e) {
            System.err.println(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String saveUser = "INSERT INTO users (name,lastname,age) VALUES (?,?,?)";
        Savepoint savepoint = null;
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(saveUser)) {
            savepoint = connection.setSavepoint();
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.executeUpdate();
            connection.commit();
            System.out.println("User с именем - " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            System.err.println(e);
            try{
                Connection con = Util.getConnection();
                con.rollback(savepoint);
            } catch (SQLException ex){
                System.err.println(ex);
            }
        }
    }

    public void removeUserById(long id) {
        String removeUserByIdSQL = "DELETE FROM users WHERE id = ?";
        Savepoint savepoint = null;
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(removeUserByIdSQL)){
            savepoint = connection.setSavepoint();
            statement.setLong(1, id);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e){
            System.err.println(e);
            try{
                Connection con = Util.getConnection();
                con.rollback(savepoint);
            } catch (SQLException ex){
                System.err.println(ex);
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String getAllUsersSQL = "SELECT * FROM users";
        try ( Connection connection = Util.getConnection();
              Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery(getAllUsersSQL)) {
            while(resultSet.next()){
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastname");
                byte age = resultSet.getByte("age");
                users.add(new User(name,lastName,age));
            }
            connection.commit();
        } catch (SQLException e) {
            System.err.println(e);
        }
        return users;
    }

    public void cleanUsersTable() {
        String cleanUsersTableSQL = "TRUNCATE TABLE users";
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(cleanUsersTableSQL);
            connection.commit();
        } catch (SQLException e) {
            System.err.println(e);
        }
    }
}
