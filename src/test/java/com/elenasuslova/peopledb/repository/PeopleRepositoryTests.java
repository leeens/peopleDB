package com.elenasuslova.peopledb.repository;

import com.elenasuslova.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {
    private Connection connection;
    private PeopleRepository repo;
    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/peopletest".replace("~", System.getProperty("user.home")));
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException{
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson(){
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople(){
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1986, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        assertThat(savedPerson.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById(){
        Person savedPerson = repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIDNotFound(){
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canFindAll() {
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John2", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John3", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John4", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John5", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John6", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John7", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John8", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        List<Person> people = repo.findAll();
        assertThat(people.size()).isGreaterThanOrEqualTo(10);
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        repo.save(new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete(){
        Person savedPerson = repo.save(new Person("Bobby", "Smith", ZonedDateTime.of(1986, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople(){
        Person p1 = repo.save(new Person("Bobby1", "Smith", ZonedDateTime.of(1986, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p2 = repo.save(new Person("Bobby2", "Smith", ZonedDateTime.of(1986, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        repo.delete(p1, p2);
    }

    @Test
    public void canUpdate(){
        Person savedPerson = repo.save(new Person("Bobby", "Smith", ZonedDateTime.of(1986, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        Person p1 = repo.findById(savedPerson.getId()).get();
        savedPerson.setSalary(new BigDecimal("730000.28"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();
        assertThat(p2.getSalary()).isNotEqualTo(p1.getSalary());

    }

    @Test
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("/Users/elenasuslova/Downloads/Hr5m.csv"))
                .skip(1)
                .map(l -> l.split(","))
                .map(a -> {
                    LocalDate dob = LocalDate.parse(a[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(a[11], DateTimeFormatter.ofPattern(("hh:mm:ss a")));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(a[2], a[4], zdtob);
                    person.setSalary(new BigDecimal(a[25]));
                    person.setEmail(a[6]);
                    return person;
                })
                .forEach(repo::save);
        connection.commit();
    }
}
