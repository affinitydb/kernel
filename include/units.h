/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#ifndef _UNITS_H_
#define _UNITS_H_

namespace MVStore
{
	enum Units 
	{
		Un_NDIM,
		// base SI units
		Un_m,					// distance - meter
		Un_kg,					// mass	- kilogram
		Un_s,					// duration - second
		Un_A,					// current - Ampere
		Un_K,					// temperature - Kelvin
		Un_mol,					// amount of matter - mole
		Un_cd,					// intensity of light - candela
		Un_Base,

		// derived SI units
		Un_Hz=Un_Base,			// frequency - Hertz
		Un_N,					// force - Newton
		Un_Pa,					// pressure - Pascal
		Un_J,					// energy or work - Joule
		Un_W,					// power - Watt
		Un_C,					// electric charge - Coulomb
		Un_V,					// electric potential - Volt
		Un_F,					// electric capacitance - Farad
		Un_Ohm,					// electric resistance - Ohm
		Un_S,					// electric conductance - Siemens
		Un_Wb,					// magnetic flux - Weber
		Un_T,					// magnetic flux density - Tesla
		Un_H,					// inductance - Henry
		Un_dC,					// temperature - degree Celsius (°C)
		Un_rad,					// plane angle - radian
		Un_sr,					// solid angle - steradian
		Un_lm,					// luminous - lumen
		Un_lx,					// illuminance - lux
		Un_Bq,					// activity /s - Becquerel
		Un_Gy,					// absorbed dose - Gray
		Un_Sv,					// dose equivalent - Sievert
		Un_kat,					// catalytic activity - katal
		Un_Derived,

		// metric length
		Un_dm=Un_Derived,		// decimeter
		Un_cm,					// centimeter
		Un_mm,					// millimeter
		Un_mkm,					// micrometer
		Un_nm,					// nanometer
		Un_km,					// kilometer

		// English length
		Un_in,					// inch
		Un_ft,					// foot
		Un_yd,					// yard
		Un_mi,					// stature mile
		Un_nmi,					// nautical mile

		// astronomical distance
		Un_au,					// astronomical unit
		Un_pc,					// parsec
		Un_ly,					// light year

		// speed
		Un_mps,					// meters per second
		Un_kph,					// kilometers per hour
		Un_fpm,					// feet per minute
		Un_mph,					// miles per hour
		Un_kt,					// knot (nautical miles per hour)

		// weight
		Un_g,					// gram
		Un_mg,					// milligram
		Un_mkg,					// microgram
		Un_t,					// metric ton
		Un_lb,					// pound
		Un_oz,					// once
		Un_st,					// stone

		// area
		Un_m2,					// square meter
		Un_cm2,					// square centimeter
		Un_sq_in,				// square inch
		Un_sq_ft,				// square foot
		Un_ac,					// acre
		Un_ha,					// hectare

		// volume
		Un_m3,					// cubic meter
		Un_L,					// liter
		Un_cl,					// centiliter
		Un_cm3,					// cubic centimeter
		Un_cf,					// cubic foot
		Un_ci,					// cubic inch
		Un_fl_oz,				// fluid once
		Un_bbl,					// barrel
		Un_bu,					// bushel
		Un_gal,					// gallon
		Un_qt,					// quarter
		Un_pt,					// pint

		// pressure
		Un_b,					// bar
		Un_mmHg,				// millimeters of mercury
		Un_inHg,				// inches of mercury

		// various
		Un_cal,					// calorie
		Un_kcal,				// Calorie
		Un_ct,					// carat
		Un_dF,					// degrees Fahrenheit (°F)

		Un_ALL
	};
};

#endif
