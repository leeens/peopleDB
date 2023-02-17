package com.elenasuslova.peopledb.repository;

import com.elenasuslova.peopledb.annotation.SQL;
import com.elenasuslova.peopledb.model.Address;
import com.elenasuslova.peopledb.model.CrudOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CrudRepository<Address> {

    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        return null;
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value= """
           INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY,
           REGION, COUNTRY) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
           """)
    void mapForSave(Address entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.streetAddress());
        ps.setString(2, entity.address2());
        ps.setString(3, entity.city());
        ps.setString(4, entity.state());
        ps.setString(5, entity.postcode());
        ps.setString(6, entity.county());
        ps.setString(7, entity.region().toString());
        ps.setString(8, entity.country());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }
}
