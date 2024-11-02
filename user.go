package main

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
)

const FlagUltraHDUploads int64 = 16 // joke flag

type User struct {
	Username string `bson:"_id"`
	Flags    int64  `bson:"flags"`
}

func getUserByToken(token string) (*User, error) {
	var user User
	err := db.Collection("usersv0").FindOne(
		context.TODO(),
		bson.M{"tokens": token},
	).Decode(&user)
	return &user, err
}
