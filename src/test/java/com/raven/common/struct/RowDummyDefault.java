/* 
 * Copyright (C) 2019 Raven Computing
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.raven.common.struct;

import com.raven.common.struct.Row;
import com.raven.common.struct.RowItem;

/**
 * Dummy class for row annotation tests with DefaultDataFrames.
 * 
 * @author Phil Gaiser
 *
 */
public class RowDummyDefault implements Row {

	@RowItem("BYTE")
	private byte mByte = 1;
	
	@RowItem("SHORT")
	private short mShort = 2;
	
	@RowItem("INT")
	private int mInt = 3;
	
	@RowItem("LONG")
	private long mLong = 4l;
	
	@RowItem("STRING")
	private String mString = "A5";
	
	@RowItem("CHAR")
	private char mChar = 'B';
	
	@RowItem("FLOAT")
	private float mFloat = 7.2f;
	
	@RowItem("DOUBLE")
	private double mDouble = 8.3;
	
	@RowItem("BOOLEAN")
	private boolean mBoolean = true;
	
	public RowDummyDefault(){
		
	}
	
	public RowDummyDefault(byte mByte, short mShort, int mInt, long mLong, String mString, Character mChar,
			float mFloat, double mDouble, boolean mBoolean){
		
		this.mByte = mByte;
		this.mShort = mShort;
		this.mInt = mInt;
		this.mLong = mLong;
		this.mString = mString;
		this.mChar = mChar;
		this.mFloat = mFloat;
		this.mDouble = mDouble;
		this.mBoolean = mBoolean;
	}

	public byte getmByte(){
		return mByte;
	}

	public void setmByte(byte mByte){
		this.mByte = mByte;
	}

	public short getmShort(){
		return mShort;
	}

	public void setmShort(short mShort){
		this.mShort = mShort;
	}

	public int getmInt(){
		return mInt;
	}

	public void setmInt(int mInt){
		this.mInt = mInt;
	}

	public long getmLong(){
		return mLong;
	}

	public void setmLong(long mLong){
		this.mLong = mLong;
	}

	public String getmString(){
		return mString;
	}

	public void setmString(String mString){
		this.mString = mString;
	}

	public Character getmChar(){
		return mChar;
	}

	public void setmChar(Character mChar){
		this.mChar = mChar;
	}

	public float getmFloat(){
		return mFloat;
	}

	public void setmFloat(float mFloat){
		this.mFloat = mFloat;
	}

	public double getmDouble(){
		return mDouble;
	}

	public void setmDouble(double mDouble){
		this.mDouble = mDouble;
	}

	public boolean ismBoolean(){
		return mBoolean;
	}

	public void setmBoolean(boolean mBoolean){
		this.mBoolean = mBoolean;
	}

}
