/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ******************************************************************************/
package com.denodo.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.denodo.connect.business.services.metadata.MetadataService;

@Component
public class DenodoDataStore {
	
	private static final Logger logger = Logger.getLogger(DenodoDataStore.class);

	@Autowired
	private MetadataService metadataService;	// Data accessors

	

	public Map<String, Object> getCar(final int id) {
		Map<String, Object> data = null;

		Calendar updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

		switch (id) {
		case 1:
 			updated.set(2012, 11, 11, 11, 11, 11);
			data = createCar(1, "F1 W03", 1, 189189.43, "EUR", "2012", updated, "file://imagePath/w03");
			try{
				this.metadataService.getMetadataView("final_count_by_cache_vdp_queries");
				this.metadataService.getMetadataTables();
			}
			catch (Exception e) {
				logger.error("Error metadataService",e);
			}
			//      try{
			//    data= getCarTable(getConnection());
			//    
			//      break;
			//      }
			//      catch (Exception e) {
			//		// TODO: handle exception
			//	}
			break;
		case 2:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(2, "F1 W04", 1, 199999.99, "EUR", "2013", updated, "file://imagePath/w04");
			break;

		case 3:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(3, "F2012", 2, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012");
			break;

		case 4:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(4, "F2013", 2, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013");
			break;

		case 5:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(5, "F1 W02", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX");
			break;
		case 6:
			updated.set(2014, 11, 11, 11, 11, 11);
			data = createCar(6, "F6 W06", 6, 19999.59, "EUR", "2014", updated, "file://imagePath/w06");
			break;

		case 7:
			updated.set(2012, 11, 12, 12, 12, 12);
			data = createCar(7, "F2017", 5, 537685.36, "EUR", "2012", updated, "http://pathToImage/f2007");
			break;

		case 8:
			updated.set(2013, 11, 12, 12, 12, 12);
			data = createCar(8, "F2008", 4, 145519.90, "EUR", "2013", updated, "http://pathToImage/f2015");
			break;

		case 9:
			updated.set(2011, 10, 11, 11, 11, 11);
			data = createCar(9, "F1 W09", 3, 167189.00, "EUR", "2011", updated, "file://imagePath/wX19");
			break;
		case 10:
			updated.set(2013, 9, 11, 11, 11, 11);
			data = createCar(10, "F1 W10", 1, 54667.39, "EUR", "2013", updated, "file://imagePath/w044");
			break;

		case 11:
			updated.set(2012, 10, 12, 12, 12, 12);
			data = createCar(11, "F2011", 2, 313765.33, "EUR", "2012", updated, "http://pathToImage/f2014");
			break;

		case 12:
			updated.set(2013, 9, 12, 12, 12, 12);
			data = createCar(12, "F2013", 2, 522825.00, "EUR", "2013", updated, "http://pathToImage/f2015");
			break;

		case 13:
			updated.set(2011, 7, 11, 11, 11, 11);
			data = createCar(13, "F1 W007", 1, 172189.00, "EUR", "2011", updated, "file://imagePath/wXXa");
			break;
		case 14:
			updated.set(2013, 6, 11, 11, 11, 11);
			data = createCar(14, "F1 W016", 3, 49999.94, "EUR", "2013", updated, "file://imagePath/w04s");
			break;

		case 15:
			updated.set(2012, 11, 12, 12, 12, 12);
			data = createCar(15, "F2012", 4, 527285.23, "EUR", "2012", updated, "http://pathToImage/f2014g");
			break;

		case 16:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(16, "F20133", 4, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2043k");
			break;

		case 17:
			updated.set(2011, 2, 11, 11, 11, 11);
			data = createCar(17, "F1 W0234", 5, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX2k");
			break;
		case 18:
			updated.set(2013, 1, 11, 11, 11, 11);
			data = createCar(18, "F1 W044", 5, 199999.99, "EUR", "2013", updated, "file://imagePath/w04k");
			break;

		case 19:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(19, "F20126", 5, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012g");
			break;

		case 20:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(20, "F20136", 6, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013s");
			break;

		case 21:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(21, "F1 W025", 6, 167189.00, "EUR", "2011", updated, "file://imagePath/wXfX");
			break;
		case 22:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(22, "F1 W043", 6, 199999.99, "EUR", "2013", updated, "file://imagePath/w04d");
			break;

		case 23:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(23, "F20125", 6, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012f");
			break;

		case 24:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(24, "F20163", 2, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013s");
			break;

		case 25:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(25, "F1 W023", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXXs");
			break;
		case 26:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(26, "F1 W043", 1, 199999.99, "EUR", "2013", updated, "file://imagePath/w04d");
			break;

		case 27:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(27, "F20122", 2, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012sdf");
			break;

		case 28:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(28, "F20123", 2, 145285.00, "EUR", "2013", updated, "http://pathToImage/f20139");
			break;

		case 29:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(29, "F1 W022", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX1");
			break;
		case 30:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(30, "F1 W04s", 4, 199999.99, "EUR", "2013", updated, "file://imagePath/w045");
			break;

		case 31:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(31, "F2012s", 4, 137285.33, "EUR", "2012", updated, "http://pathToImage/f201255");
			break;

		case 32:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(32, "F2013s", 3, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013567");
			break;

		case 33:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(33, "F1 W02s", 3, 167189.00, "EUR", "2011", updated, "file://imagePath/wX576X");
			break;
		case 34:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(34, "F2012s", 5, 137285.33, "EUR", "2012", updated, "http://pathToImage/f201276");
			break;

		case 35:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(35, "F2013s", 5, 145285.00, "EUR", "2013", updated, "http://pathToImage/f201376");
			break;

		case 36:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(36, "F1 W02s", 5, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX5");
			break;
		case 37:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(37, "F1 W04s", 6, 199999.99, "EUR", "2013", updated, "file://imagePath/w054");
			break;

		case 38:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(38, "F2012s", 6, 137285.33, "EUR", "2012", updated, "http://pathToImage/f201276");
			break;

		case 39:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(39, "F2013s", 3, 145285.00, "EUR", "2013", updated, "http://pathToImage/f201376");
			break;

		case 40:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(40, "F1 W02d", 3, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX45");
			break;
		case 41:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(41, "F1 W04d", 1, 199999.99, "EUR", "2013", updated, "file://imagePath/w04456");
			break;

		case 42:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(42, "F2012s", 2, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012213");
			break;

		case 43:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(43, "F2013s", 2, 145285.00, "EUR", "2013", updated, "http://pathToImage/f201313");
			break;

		case 44:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(44, "F1 W0s2", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX3");
			break;
		case 45:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(45, "F1 W04s", 4, 199999.99, "EUR", "2013", updated, "file://imagePath/w0433");
			break;

		case 46:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(46, "F201s2", 5, 137285.33, "EUR", "2012", updated, "http://pathToImage/f20123");
			break;

		case 47:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(47, "F201d3", 6, 145285.00, "EUR", "2013", updated, "http://pathToImage/f201311");
			break;

		case 48:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(48, "F1 Wf02", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX33");
			break;
		case 49:
			updated.set(2013, 11, 11, 11, 11, 11);
			data = createCar(49, "F1 Wf04", 1, 199999.99, "EUR", "2013", updated, "file://imagePath/w042");
			break;

		case 50:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(50, "Fer2012", 2, 137285.33, "EUR", "2012", updated, "http://pathToImage/f20122");
			break;

		case 51:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(51, "F20ffd13", 3, 145285.00, "EUR", "2013", updated, "http://pathToImage/f20123");
			break;

		case 52:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(52, "F1 W02", 4, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX");
			break;
		case 53:
			updated.set(2012, 12, 12, 12, 12, 12);
			data = createCar(53, "F2012", 5, 137285.33, "EUR", "2012", updated, "http://pathToImage/f2012");
			break;

		case 54:
			updated.set(2013, 12, 12, 12, 12, 12);
			data = createCar(44, "F2013", 6, 145285.00, "EUR", "2013", updated, "http://pathToImage/f2013");
			break;

		case 55:
			updated.set(2011, 11, 11, 11, 11, 11);
			data = createCar(55, "F1 W02", 1, 167189.00, "EUR", "2011", updated, "file://imagePath/wXX");
			break;

		default:
			break;
		}

		return data;
	}

	private static Map<String, Object> createCar(final int carId, final String model, final int manufacturerId,
			final double price,
			final String currency, final String modelYear, final Calendar updated, final String imagePath) {
		Map<String, Object> data = new HashMap<String, Object>();

		data.put("Id", carId);
		data.put("Model", model);
		data.put("ManufacturerId", manufacturerId);
		data.put("Price", price);
		data.put("Currency", currency);
		data.put("ModelYear", modelYear);
		data.put("Updated", updated);
		data.put("ImagePath", imagePath);

		return data;
	}

	public Map<String, Object> getManufacturer(final int id) {
		Map<String, Object> data = null;
		Calendar date = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

		switch (id) {
		case 1:
			Map<String, Object> addressStar = createAddress("Star Street 137", "Stuttgart", "70173", "Germany");
			date.set(1954, 7, 4);
			data = createManufacturer(1, "Star Powered Racing", addressStar, date);
			break;

		case 2:
			Map<String, Object> addressHorse = createAddress("Horse Street 1", "Maranello", "41053", "Italy");
			date.set(1929, 11, 16);
			data = createManufacturer(2, "Horse Powered Racing", addressHorse, date);
			break;
		case 3:
			Map<String, Object> addressCircle = createAddress("Circle Street 17", "A Coruña", "15000", "Spain");
			date.set(1958, 7, 4);
			data = createManufacturer(3, "Circle Powered Racing", addressCircle, date);
			break;

		case 4:
			Map<String, Object> addressTriangule = createAddress("Triangule Street 12", "Vigo", "36000", "Spain");
			date.set(1999, 11, 6);
			data = createManufacturer(4, "Triangule Powered Racing", addressTriangule, date);
			break;

		case 5:
			Map<String, Object> addressRectangle = createAddress("Star Street 137", "Oporto", "70173", "Portugal");
			date.set(1924, 7, 3);
			data = createManufacturer(5, "Rectangle Powered Racing", addressRectangle, date);
			break;

		case 6:
			Map<String, Object> addressSquare = createAddress("Square Street 41", "Lisboa", "41053", "portugal");
			date.set(1936, 1, 1);
			data = createManufacturer(6, "Square Powered Racing", addressSquare, date);
			break;


		default:
			break;
		}

		return data;
	}

	private Map<String, Object> createManufacturer(final int id, final String name, final Map<String, Object> address,
			final Calendar updated) {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("Id", id);
		data.put("Name", name);
		data.put("Address", address);
		data.put("Updated", updated);
		return data;
	}

	private Map<String, Object> createAddress(final String street, final String city, final String zipCode,
			final String country) {
		Map<String, Object> address = new HashMap<String, Object>();
		address.put("Street", street);
		address.put("City", city);
		address.put("ZipCode", zipCode);
		address.put("Country", country);
		return address;
	}

	public List<Map<String, Object>> getCars() throws SQLException, ClassNotFoundException {
		List<Map<String, Object>> cars = new ArrayList<Map<String, Object>>();
		cars.add(getCar(1));
		cars.add(getCar(2));
		cars.add(getCar(3));
		cars.add(getCar(4));
		cars.add(getCar(5));
		cars.add(getCar(6));
		cars.add(getCar(7));
		cars.add(getCar(8));
		cars.add(getCar(9));
		cars.add(getCar(10));
		cars.add(getCar(11));
		cars.add(getCar(12));
		cars.add(getCar(13));
		cars.add(getCar(14));
		cars.add(getCar(15));
		cars.add(getCar(16));
		cars.add(getCar(17));
		cars.add(getCar(18));
		cars.add(getCar(19));
		cars.add(getCar(20));
		cars.add(getCar(21));
		cars.add(getCar(22));
		cars.add(getCar(23));
		cars.add(getCar(24));
		cars.add(getCar(25));
		cars.add(getCar(26));
		cars.add(getCar(27));
		cars.add(getCar(28));
		cars.add(getCar(29));
		cars.add(getCar(30));
		cars.add(getCar(41));
		cars.add(getCar(42));
		cars.add(getCar(43));
		cars.add(getCar(44));
		cars.add(getCar(45));
		cars.add(getCar(46));
		cars.add(getCar(47));
		cars.add(getCar(48));
		cars.add(getCar(49));
		cars.add(getCar(50));
		cars.add(getCar(51));
		cars.add(getCar(52));
		cars.add(getCar(53));
		cars.add(getCar(54));
		cars.add(getCar(55));

		return cars;
	}

	public List<Map<String, Object>> getManufacturers() {
		List<Map<String, Object>> manufacturers = new ArrayList<Map<String, Object>>();
		manufacturers.add(getManufacturer(1));
		manufacturers.add(getManufacturer(2));
		manufacturers.add(getManufacturer(3));
		manufacturers.add(getManufacturer(4));
		manufacturers.add(getManufacturer(5));
		manufacturers.add(getManufacturer(6));
		return manufacturers;
	}

	public List<Map<String, Object>> getCarsFor(final int manufacturerId) throws SQLException, ClassNotFoundException {
		List<Map<String, Object>> cars = getCars();
		List<Map<String, Object>> carsForManufacturer = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> car : cars) {
			if (Integer.valueOf(manufacturerId).equals(car.get("ManufacturerId"))) {
				carsForManufacturer.add(car);
			}
		}

		return carsForManufacturer;
	}

	public Map<String, Object> getManufacturerFor(final int carId) {
		Map<String, Object> car = getCar(carId);
		if (car != null) {
			Object manufacturerId = car.get("ManufacturerId");
			if (manufacturerId != null) {
				return getManufacturer((Integer) manufacturerId);
			}
		}
		return null;
	}


	public static Connection getConnection() throws SQLException, ClassNotFoundException  {

		Class.forName("com.denodo.vdp.jdbc.Driver");
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "admin");
		connectionProps.put("password", "admin");

		conn = DriverManager.getConnection(
				"jdbc:vdb://localhost:9999/admin?queryTimeout=100000&chunkTimeout=1000",
				connectionProps);

		System.out.println("Connected to database");
		return conn;
	}


	public static Map<String, Object> getCarTable(Connection con)
			throws SQLException {

		Statement stmt = null;
		String query =
				"SELECT id, model, manufacturerId, price, currency, modelYear, updated, imagePath FROM cars where id = 1 ;";
		Map<String, Object> data = null;
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);


			while (rs.next()) {
				int id = rs.getInt("id");
				String modelo = rs.getString("model");
				int manufacturerId = rs.getInt("manufacturerId");
				String currency = rs.getString("currency");
				Double price = (double) rs.getInt("price");
				String modelYear =String.valueOf(rs.getInt("modelYear"));
				String imagePath = rs.getString("imagePath");
				System.out.println(id + "\t" + modelo +
						"\t" + manufacturerId + "\t" + currency +
						"\t" + modelYear + "\t" +
						imagePath);
				Calendar updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
				updated.set(2013, 11, 11, 11, 11, 11);
				data=createCar(id,modelo,manufacturerId,price,currency,modelYear,updated,imagePath);
			}


		} catch (SQLException e ) {
			System.out.println(e.getMessage());
		} finally {
			if (stmt != null) { stmt.close(); }
		}
		return data;

	}
	public static Map<String, Object> getInfo(Connection con)
			throws SQLException {

		Statement stmt = null;
		String query =
				"DESC VIEW final_count_by_username_vdp_queries ;";
		Map<String, Object> data = null;
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			System.out.println("executed");
			String a = rs.getString(0);
			System.out.println(a);
			while (rs.next()) {
				int id = rs.getInt("id");
				String modelo = rs.getString("model");
				int manufacturerId = rs.getInt("manufacturerId");
				String currency = rs.getString("currency");
				Double price = (double) rs.getInt("price");
				String modelYear =String.valueOf(rs.getInt("modelYear"));
				String imagePath = rs.getString("imagePath");
				System.out.println(id + "\t" + modelo +
						"\t" + manufacturerId + "\t" + currency +
						"\t" + modelYear + "\t" +
						imagePath);
				Calendar updated = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
				updated.set(2013, 11, 11, 11, 11, 11);
				data=createCar(id,modelo,manufacturerId,price,currency,modelYear,updated,imagePath);
			}


		} catch (SQLException e ) {
			System.out.println(e.getMessage());
		} finally {
			if (stmt != null) { stmt.close(); }
		}

		return data;

	}


	public  MetadataService getMetadataService() {
		return metadataService;
	}
}
