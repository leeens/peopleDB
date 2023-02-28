package com.elenasuslova.peopledb.repository;

import com.elenasuslova.peopledb.annotation.SQL;
import com.elenasuslova.peopledb.model.Address;
import com.elenasuslova.peopledb.model.CrudOperation;
import com.elenasuslova.peopledb.model.Person;
import com.elenasuslova.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class PeopleRepository extends CrudRepository<Person> {

    public static final String SAVE_PERSON_SQL = """
                INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BIZ_ADDRESS)  
                VALUES(?, ?, ?, ?, ?, ?, ?)""";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT (*) FROM PEOPLE";
    public static final String FIND_BY_ID_SQL = """
    SELECT 
    P.ID, P.FIRST_NAME, P.LAST_NAME, P.DOB, P.SALARY, P.HOME_ADDRESS, 
    HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
    BIZ.ID AS BIZ_ID, BIZ.STREET_ADDRESS AS BIZ_STREET_ADDRESS, BIZ.ADDRESS2 AS BIZ_ADDRESS2, BIZ.CITY AS BIZ_CITY, BIZ.STATE AS BIZ_STATE, BIZ.POSTCODE AS BIZ_POSTCODE, BIZ.COUNTY AS BIZ_COUNTY, BIZ.REGION AS BIZ_REGION, BIZ.COUNTRY AS BIZ_COUNTRY
    FROM PEOPLE AS P
    LEFT OUTER JOIN ADDRESSES AS HOME ON P.HOME_ADDRESS = HOME.ID
    LEFT OUTER JOIN ADDRESSES AS BIZ ON P.BIZ_ADDRESS = BIZ.ID
    WHERE P.ID=?""";

    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    public static final String DELETE_ONE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public AddressRepository addressRepository;
    public PeopleRepository(Connection connection) {

        super(connection);
        addressRepository = new AddressRepository(connection);
    }
    @Override
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_ONE_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException{
        long personId = rs.getLong("ID");
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of((rs.getTimestamp("DOB").toLocalDateTime()), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("SALARY");
        long homeAddressId = rs.getLong("HOME_ADDRESS");

        Address homeAddress = extractAddress(rs, "HOME_");
        Address bizAddress = extractAddress(rs, "BIZ_");

        Person person = new Person(personId, firstName, lastName, dob, salary);
        person.setHomeAddress(homeAddress);
        person.setBusinessAddress(bizAddress);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        Long addressId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);
        if (addressId == null) return null;
        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", rs, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", rs, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", rs, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", rs, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", rs, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", rs, String.class);
        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
    }

    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int colIdx=1; colIdx<=columnCount; colIdx++){
            if(alias.equals(rs.getMetaData().getColumnLabel(colIdx))){
                return (T) rs.getObject(colIdx);
            }
        }
        throw new SQLException(String.format("Column not found for alias: '%s'", alias));
    }


    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }
    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        Address savedAddress = null;
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        associateAddressWithPerson(entity.getHomeAddress(), ps, 6);
        associateAddressWithPerson(entity.getBusinessAddress(), ps, 7);
    }

    private void associateAddressWithPerson(Optional<Address> address, PreparedStatement ps, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    private static Timestamp convertDobToTimeStamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob
                .withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
