const userList = [
    "Harald",
    "Astrid",
    "Liv",
    "Floki",
    "Thor",
    "Thora",
    "Frigg",
    "Viggo"
]

export const getUser =  () => {
     return userList[Math.floor(Math.random() * (userList.length))];
}