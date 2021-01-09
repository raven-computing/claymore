/* 
 * Copyright (C) 2021 Raven Computing
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

/**
 * Dummy class for row annotation tests with NullableDataFrames.
 *
 */
public class RowDummyNullable implements Row {

    @RowItem("BYTE")
    private Byte mByte = 1;

    @RowItem("SHORT")
    private Short mShort = 2;

    @RowItem("INT")
    private Integer mInt = 3;

    @RowItem("LONG")
    private Long mLong = 4l;

    @RowItem("STRING")
    private String mString = "A5";

    @RowItem("CHAR")
    private Character mChar = 'B';

    @RowItem("FLOAT")
    private Float mFloat = 7.2f;

    @RowItem("DOUBLE")
    private Double mDouble = 8.3;

    @RowItem("BOOLEAN")
    private Boolean mBoolean = true;

    public RowDummyNullable(){

    }

    public RowDummyNullable(Byte mByte, Short mShort, Integer mInt, Long mLong, String mString, Character mChar,
            Float mFloat, Double mDouble, Boolean mBoolean){

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

    public Byte getmByte(){
        return mByte;
    }

    public void setmByte(Byte mByte){
        this.mByte = mByte;
    }

    public Short getmShort(){
        return mShort;
    }

    public void setmShort(Short mShort){
        this.mShort = mShort;
    }

    public Integer getmInt(){
        return mInt;
    }

    public void setmInt(Integer mInt){
        this.mInt = mInt;
    }

    public Long getmLong(){
        return mLong;
    }

    public void setmLong(Long mLong){
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

    public Float getmFloat(){
        return mFloat;
    }

    public void setmFloat(Float mFloat){
        this.mFloat = mFloat;
    }

    public Double getmDouble(){
        return mDouble;
    }

    public void setmDouble(Double mDouble){
        this.mDouble = mDouble;
    }

    public Boolean getmBoolean(){
        return mBoolean;
    }

    public void setmBoolean(Boolean mBoolean){
        this.mBoolean = mBoolean;
    }

}
