package com.elenasuslova.peopledb.model;

import com.elenasuslova.peopledb.annotation.Id;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Person{
    @Id
    private Long id;
    String firstName;
    String lastName;
    ZonedDateTime dob;
    private BigDecimal salary = new BigDecimal(0);
    public Person(long id, String firstName, String lastName, ZonedDateTime dob, BigDecimal salary) {
        this(id, firstName, lastName, dob);
        this.salary = salary;
    }
    public Person(Long id, String firstName, String lastName, ZonedDateTime dob) {
        this(firstName, lastName, dob);
        this.id = id;
    }
    public Person(String firstName, String lastName, ZonedDateTime dob) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.dob = dob;
    }
    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public ZonedDateTime getDob() {
        return dob;
    }
    public void setDob(ZonedDateTime dob) {
        this.dob = dob;
    }
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public BigDecimal getSalary() {
        return salary;
    }
    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dob=" + dob +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Person person)) return false;
        return Objects.equals(id, person.id) && firstName.equals(person.firstName) && lastName.equals(person.lastName) &&
                dob.withZoneSameInstant(ZoneId.of("+0")).equals(person.dob.withZoneSameInstant(ZoneId.of("+0")));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, dob);
    }

}