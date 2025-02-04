export const getNumberMessage = (configNumber) => {
    const {integerSize, decimalValue, signed} = configNumber
    const signe = signed && (Math.floor(Math.random() * 100) % 2) ? -1 : 1
    let number = Math.random() * (Math.pow(10, integerSize + decimalValue)) * signe
    number = decimalValue === 0 ? Math.floor(number) : Math.floor(number) / Math.pow(10, decimalValue)

    return number
}